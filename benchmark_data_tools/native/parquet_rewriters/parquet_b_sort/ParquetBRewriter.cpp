#include <cudf/aggregation.hpp>
#include <cudf/hashing.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/sorting.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include <rmm/cuda_stream.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace fs = std::filesystem;

using Policy =
    std::map<std::string, std::map<std::string, cudf::io::column_encoding>>;

struct Options {
  fs::path inputRoot;
  fs::path outputRoot;
  fs::path policyPath;
  std::size_t shardIndex{0};
  std::size_t shardCount{1};
  std::set<std::string> tables;
  std::map<std::string, std::vector<std::string>> sortColumns;
  std::optional<std::size_t> maxFiles;
  std::size_t rowGroupSizeRows{2'000'000};
  bool verify{true};
};

struct Timer {
  Timer() : start(std::chrono::steady_clock::now()) {}

  double seconds() const {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now() - start)
        .count();
  }

  std::chrono::steady_clock::time_point start;
};

std::string jsonString(std::string_view value) {
  std::string output{"\""};
  for (char c : value) {
    switch (c) {
      case '\\':
        output += "\\\\";
        break;
      case '"':
        output += "\\\"";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        output += c;
    }
  }
  output += '"';
  return output;
}

std::size_t parseSize(std::string const& name, std::string const& value) {
  std::size_t consumed = 0;
  auto parsed = std::stoull(value, &consumed);
  if (consumed != value.size()) {
    throw std::runtime_error(
        fmt::format("{} must be an unsigned integer", name));
  }
  return parsed;
}

Options parseOptions(int argc, char** argv) {
  Options options;
  auto value = [&](int& index) -> std::string {
    if (++index >= argc) {
      throw std::runtime_error(
          fmt::format("{} requires a value", argv[index - 1]));
    }
    return argv[index];
  };
  for (int i = 1; i < argc; ++i) {
    std::string const arg = argv[i];
    if (arg == "--input-root") {
      options.inputRoot = value(i);
    } else if (arg == "--output-root") {
      options.outputRoot = value(i);
    } else if (arg == "--policy") {
      options.policyPath = value(i);
    } else if (arg == "--shard-index") {
      options.shardIndex = parseSize(arg, value(i));
    } else if (arg == "--shard-count") {
      options.shardCount = parseSize(arg, value(i));
    } else if (arg == "--table") {
      options.tables.insert(value(i));
    } else if (arg == "--sort") {
      auto const spec = value(i);
      auto const separator = spec.find(':');
      if (separator == std::string::npos || separator == 0 ||
          separator + 1 == spec.size()) {
        throw std::runtime_error(
            "--sort must have the form table:column[,column...]");
      }
      auto const table = spec.substr(0, separator);
      std::vector<std::string> columns;
      std::size_t begin = separator + 1;
      while (begin < spec.size()) {
        auto const end = spec.find(',', begin);
        auto const column = spec.substr(begin, end - begin);
        if (column.empty()) {
          throw std::runtime_error("--sort contains an empty column name");
        }
        columns.push_back(column);
        if (end == std::string::npos) {
          break;
        }
        begin = end + 1;
      }
      if (!options.sortColumns.emplace(table, std::move(columns)).second) {
        throw std::runtime_error(
            fmt::format("duplicate --sort specification for {}", table));
      }
    } else if (arg == "--max-files") {
      options.maxFiles = parseSize(arg, value(i));
    } else if (arg == "--row-group-size-rows") {
      options.rowGroupSizeRows = parseSize(arg, value(i));
    } else if (arg == "--no-verify") {
      options.verify = false;
    } else {
      throw std::runtime_error(fmt::format("unknown option {}", arg));
    }
  }
  if (options.inputRoot.empty() || options.outputRoot.empty() ||
      options.policyPath.empty()) {
    throw std::runtime_error(
        "--input-root, --output-root, and --policy are required");
  }
  options.inputRoot = fs::canonical(options.inputRoot);
  options.outputRoot = fs::absolute(options.outputRoot).lexically_normal();
  options.policyPath = fs::canonical(options.policyPath);
  if (options.inputRoot == options.outputRoot) {
    throw std::runtime_error("input and output roots must differ");
  }
  if (options.shardCount == 0 || options.shardIndex >= options.shardCount) {
    throw std::runtime_error("shard-index must be in [0, shard-count)");
  }
  if (options.maxFiles.has_value() && *options.maxFiles == 0) {
    throw std::runtime_error("max-files must be positive");
  }
  if (options.rowGroupSizeRows == 0) {
    throw std::runtime_error("row-group-size-rows must be positive");
  }
  for (auto const& [table, unused] : options.sortColumns) {
    if (!options.tables.empty() && !options.tables.contains(table)) {
      throw std::runtime_error(fmt::format(
          "sort table {} is excluded by the --table selection", table));
    }
  }
  return options;
}

cudf::io::column_encoding parseEncoding(std::string_view value) {
  using cudf::io::column_encoding;
  if (value == "default") {
    return column_encoding::USE_DEFAULT;
  }
  if (value == "dictionary") {
    return column_encoding::DICTIONARY;
  }
  if (value == "plain") {
    return column_encoding::PLAIN;
  }
  if (value == "delta-binary") {
    return column_encoding::DELTA_BINARY_PACKED;
  }
  if (value == "delta-length") {
    return column_encoding::DELTA_LENGTH_BYTE_ARRAY;
  }
  if (value == "delta-byte-array") {
    return column_encoding::DELTA_BYTE_ARRAY;
  }
  if (value == "byte-stream-split") {
    return column_encoding::BYTE_STREAM_SPLIT;
  }
  throw std::runtime_error(
      fmt::format("unsupported policy encoding {}", value));
}

Policy loadPolicy(fs::path const& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error(
        fmt::format("failed to open policy {}", path.string()));
  }
  Policy policy;
  std::string line;
  std::size_t lineNumber = 0;
  std::size_t columns = 0;
  while (std::getline(input, line)) {
    ++lineNumber;
    if (line.empty() || line.front() == '#') {
      continue;
    }
    auto const first = line.find('\t');
    auto const second =
        first == std::string::npos ? first : line.find('\t', first + 1);
    if (first == std::string::npos || second == std::string::npos ||
        line.find('\t', second + 1) != std::string::npos) {
      throw std::runtime_error(
          fmt::format("{}:{}: expected three TSV fields", path.string(), lineNumber));
    }
    auto const table = line.substr(0, first);
    auto const column = line.substr(first + 1, second - first - 1);
    auto const encoding = line.substr(second + 1);
    if (!policy[table].emplace(column, parseEncoding(encoding)).second) {
      throw std::runtime_error(
          fmt::format("{}:{}: duplicate {}.{}", path.string(), lineNumber, table, column));
    }
    ++columns;
  }
  if (columns != 53) {
    throw std::runtime_error(
        fmt::format("B policy must contain exactly 53 columns, got {}", columns));
  }
  return policy;
}

std::vector<std::pair<std::string, fs::path>> inputFiles(
    Options const& options,
    Policy const& policy) {
  std::vector<std::pair<std::string, fs::path>> files;
  for (auto const& [table, unused] : policy) {
    if (!options.tables.empty() && !options.tables.contains(table)) {
      continue;
    }
    auto const tablePath = options.inputRoot / table;
    if (!fs::is_directory(tablePath)) {
      throw std::runtime_error(
          fmt::format("missing input table directory {}", tablePath.string()));
    }
    std::vector<fs::path> tableFiles;
    for (auto const& entry : fs::directory_iterator(tablePath)) {
      if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
        tableFiles.push_back(entry.path());
      }
    }
    std::sort(tableFiles.begin(), tableFiles.end());
    if (tableFiles.empty()) {
      throw std::runtime_error(
          fmt::format("no Parquet files in {}", tablePath.string()));
    }
    for (auto const& path : tableFiles) {
      files.emplace_back(table, path);
    }
  }
  for (auto const& table : options.tables) {
    if (!policy.contains(table)) {
      throw std::runtime_error(
          fmt::format("requested table {} is absent from policy", table));
    }
  }
  return files;
}

cudf::io::table_with_metadata readTable(
    fs::path const& path,
    rmm::cuda_stream_view stream) {
  auto options = cudf::io::parquet_reader_options::builder(
                     cudf::io::source_info(path.string()))
                     .build();
  auto result = cudf::io::read_parquet(options, stream);
  stream.synchronize();
  return result;
}

uint64_t aggregateChecksum(
    cudf::table_view const& table,
    rmm::cuda_stream_view stream) {
  auto hashes = cudf::hashing::xxhash_64(table, 0, stream);
  auto aggregation = cudf::make_sum_aggregation<cudf::reduce_aggregation>();
  auto sum = cudf::reduce(
      hashes->view(),
      *aggregation,
      cudf::data_type{cudf::type_id::UINT64},
      stream);
  stream.synchronize();
  auto const* typed =
      dynamic_cast<cudf::numeric_scalar<uint64_t> const*>(sum.get());
  if (typed == nullptr || !typed->is_valid(stream)) {
    throw std::runtime_error("hash-sum aggregation did not return UINT64");
  }
  return typed->value(stream);
}

std::vector<std::string> columnNames(
    cudf::io::table_metadata const& metadata) {
  std::vector<std::string> names;
  names.reserve(metadata.schema_info.size());
  for (auto const& column : metadata.schema_info) {
    names.push_back(column.name);
  }
  return names;
}

std::vector<cudf::data_type> columnTypes(cudf::table_view const& table) {
  std::vector<cudf::data_type> types;
  types.reserve(table.num_columns());
  for (cudf::size_type i = 0; i < table.num_columns(); ++i) {
    types.push_back(table.column(i).type());
  }
  return types;
}

std::string jsonStrings(std::vector<std::string> const& values) {
  std::string output{"["};
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index != 0) {
      output += ',';
    }
    output += jsonString(values[index]);
  }
  output += ']';
  return output;
}

void atomicWrite(fs::path const& path, std::string const& contents) {
  auto partial = path;
  partial += ".partial";
  {
    std::ofstream output(partial, std::ios::trunc);
    if (!output) {
      throw std::runtime_error(
          fmt::format("failed to open {}", partial.string()));
    }
    output << contents << '\n';
    if (!output) {
      throw std::runtime_error(
          fmt::format("failed to write {}", partial.string()));
    }
  }
  fs::rename(partial, path);
}

std::string markerName(fs::path const& relative) {
  auto name = relative.string();
  std::replace(name.begin(), name.end(), '/', '-');
  return name + ".json";
}

} // namespace

int main(int argc, char** argv) {
  try {
    auto const options = parseOptions(argc, argv);
    auto const policy = loadPolicy(options.policyPath);
    auto const allFiles = inputFiles(options, policy);
    std::vector<std::pair<std::string, fs::path>> assigned;
    for (std::size_t index = 0; index < allFiles.size(); ++index) {
      if (index % options.shardCount == options.shardIndex) {
        assigned.push_back(allFiles[index]);
      }
    }
    if (options.maxFiles.has_value() &&
        assigned.size() > *options.maxFiles) {
      assigned.resize(*options.maxFiles);
    }

    auto const stateDir = options.outputRoot / "_rewrite_state";
    fs::create_directories(stateDir);
    rmm::cuda_stream stream;
    Timer overall;
    uint64_t totalRows = 0;
    uint64_t sourceBytes = 0;
    uint64_t outputBytes = 0;
    std::size_t completed = 0;
    std::size_t skipped = 0;

    for (std::size_t sequence = 0; sequence < assigned.size(); ++sequence) {
      auto const& [tableName, source] = assigned[sequence];
      auto const relative = fs::relative(source, options.inputRoot);
      auto const destination = options.outputRoot / relative;
      auto const marker = stateDir / markerName(relative);
      fs::create_directories(destination.parent_path());
      if (fs::exists(marker) && fs::exists(destination)) {
        ++skipped;
        std::cout << fmt::format(
                         "{{\"kind\":\"skip\",\"shard\":{},\"file\":{}}}",
                         options.shardIndex,
                         jsonString(relative.string()))
                  << std::endl;
        continue;
      }
      if (fs::exists(destination)) {
        throw std::runtime_error(fmt::format(
            "{} exists without a completion marker; refusing overwrite",
            destination.string()));
      }

      auto partial = destination;
      partial += fmt::format(".partial-{}", options.shardIndex);
      if (fs::exists(partial)) {
        fs::remove(partial);
      }
      Timer item;
      Timer readTimer;
      auto sourceTable = readTable(source, stream.view());
      auto const readSeconds = readTimer.seconds();
      auto const names = columnNames(sourceTable.metadata);
      auto const types = columnTypes(sourceTable.tbl->view());
      if (names.size() != static_cast<std::size_t>(
                              sourceTable.tbl->num_columns())) {
        throw std::runtime_error(
            fmt::format("{} has incomplete column metadata", source.string()));
      }
      auto const& tablePolicy = policy.at(tableName);
      for (auto const& [column, unused] : tablePolicy) {
        if (std::find(names.begin(), names.end(), column) == names.end()) {
          throw std::runtime_error(fmt::format(
              "{} policy column {} is absent from physical schema",
              tableName,
              column));
        }
      }

      std::vector<std::string> sortColumns;
      double sortSeconds = 0;
      if (auto const sortIt = options.sortColumns.find(tableName);
          sortIt != options.sortColumns.end()) {
        sortColumns = sortIt->second;
        auto const values = sourceTable.tbl->view();
        std::vector<cudf::column_view> keys;
        keys.reserve(sortColumns.size());
        for (auto const& sortColumn : sortColumns) {
          auto const nameIt = std::find(names.begin(), names.end(), sortColumn);
          if (nameIt == names.end()) {
            throw std::runtime_error(fmt::format(
                "{} sort column {} is absent from physical schema",
                tableName,
                sortColumn));
          }
          keys.push_back(values.column(
              static_cast<cudf::size_type>(nameIt - names.begin())));
        }
        Timer sortTimer;
        sourceTable.tbl = cudf::sort_by_key(
            values,
            cudf::table_view{keys},
            {},
            {},
            stream.view(),
            cudf::get_current_device_resource_ref());
        stream.synchronize();
        sortSeconds = sortTimer.seconds();
      }

      auto metadata = cudf::io::table_input_metadata(sourceTable.metadata);
      std::size_t defaultColumns = 0;
      for (std::size_t column = 0; column < names.size(); ++column) {
        auto const found = tablePolicy.find(names[column]);
        if (found == tablePolicy.end()) {
          ++defaultColumns;
          metadata.column_metadata.at(column).set_encoding(
              cudf::io::column_encoding::USE_DEFAULT);
        } else {
          metadata.column_metadata.at(column).set_encoding(found->second);
        }
      }
      auto const rows = static_cast<uint64_t>(sourceTable.tbl->num_rows());
      auto const sourceChecksum =
          aggregateChecksum(sourceTable.tbl->view(), stream.view());

      auto compressionStats =
          std::make_shared<cudf::io::writer_compression_statistics>();
      auto writerOptions = cudf::io::parquet_writer_options::builder(
                               cudf::io::sink_info(partial.string()),
                               sourceTable.tbl->view())
                               .metadata(std::move(metadata))
                               .compression(cudf::io::compression_type::SNAPPY)
                               .write_v2_headers(true)
                               .page_level_compression(true)
                               .row_group_size_rows(options.rowGroupSizeRows)
                               .max_page_size_bytes(1'048'576)
                               .dictionary_policy(
                                   cudf::io::dictionary_policy::ALWAYS)
                               .max_dictionary_size(1'048'576)
                               .compression_statistics(compressionStats)
                               .build();
      Timer writeTimer;
      cudf::io::write_parquet(writerOptions, stream.view());
      stream.synchronize();
      auto const writeSeconds = writeTimer.seconds();
      sourceTable.tbl.reset();

      double verifySeconds = 0;
      uint64_t outputChecksum = sourceChecksum;
      if (options.verify) {
        Timer verifyTimer;
        auto outputTable = readTable(partial, stream.view());
        verifySeconds = verifyTimer.seconds();
        auto const outputNames = columnNames(outputTable.metadata);
        auto const outputTypes = columnTypes(outputTable.tbl->view());
        if (outputNames != names) {
          throw std::runtime_error(
              fmt::format("{} output column names changed", relative.string()));
        }
        if (outputTypes != types) {
          throw std::runtime_error(
              fmt::format("{} output column types changed", relative.string()));
        }
        if (static_cast<uint64_t>(outputTable.tbl->num_rows()) != rows) {
          throw std::runtime_error(
              fmt::format("{} output row count changed", relative.string()));
        }
        outputChecksum =
            aggregateChecksum(outputTable.tbl->view(), stream.view());
        if (outputChecksum != sourceChecksum) {
          throw std::runtime_error(
              fmt::format("{} output checksum changed", relative.string()));
        }
      }

      fs::rename(partial, destination);
      auto const inBytes = static_cast<uint64_t>(fs::file_size(source));
      auto const outBytes =
          static_cast<uint64_t>(fs::file_size(destination));
      auto const record = fmt::format(
          "{{\"kind\":\"complete\",\"shard\":{},\"sequence\":{},"
          "\"assigned_files\":{},\"file\":{},\"rows\":{},\"columns\":{},"
          "\"default_columns_outside_B\":{},\"source_bytes\":{},"
          "\"output_bytes\":{},\"xxhash64_sum\":{},"
          "\"verified_xxhash64_sum\":{},\"read_seconds\":{:.6f},"
          "\"sort_columns\":{},\"sort_seconds\":{:.6f},"
          "\"write_seconds\":{:.6f},\"verify_seconds\":{:.6f},"
          "\"total_seconds\":{:.6f}}}",
          options.shardIndex,
          sequence + 1,
          assigned.size(),
          jsonString(relative.string()),
          rows,
          names.size(),
          defaultColumns,
          inBytes,
          outBytes,
          sourceChecksum,
          outputChecksum,
          readSeconds,
          jsonStrings(sortColumns),
          sortSeconds,
          writeSeconds,
          verifySeconds,
          item.seconds());
      atomicWrite(marker, record);
      std::cout << record << std::endl;
      ++completed;
      totalRows += rows;
      sourceBytes += inBytes;
      outputBytes += outBytes;
    }

    auto const summary = fmt::format(
        "{{\"kind\":\"summary\",\"shard\":{},\"shard_count\":{},"
        "\"assigned_files\":{},\"completed_files\":{},\"skipped_files\":{},"
        "\"rows\":{},\"source_bytes\":{},\"output_bytes\":{},"
        "\"elapsed_seconds\":{:.6f}}}",
        options.shardIndex,
        options.shardCount,
        assigned.size(),
        completed,
        skipped,
        totalRows,
        sourceBytes,
        outputBytes,
        overall.seconds());
    atomicWrite(
        stateDir / fmt::format("shard-{}-summary.json", options.shardIndex),
        summary);
    std::cout << summary << std::endl;
    return 0;
  } catch (std::exception const& error) {
    std::cerr << "parquet_b_rewriter: " << error.what() << '\n';
    return 1;
  }
}
