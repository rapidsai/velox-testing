# S3 kTLS-to-CUDA reference

This directory is an executable reference for the measured S3-to-GPU path on
`g7e.48xlarge`. The runner establishes and HEAD-primes a persistent HTTPS/kTLS
connection pool without transferring object payload, then measures Range GETs
into CUDA-registered host slots and on to final GPU allocations.

The runner documents the exact NIC, IRQ, CPU, and GPU profile. It is not a
portable autotuner. Retune the profile before using another instance shape.

## Experimental ENA large-RX patch

`ena-large-rx-page-order2.patch` applies to the official
`ena_linux_2.17.2` tag from
[`amzn/amzn-drivers`](https://github.com/amzn/amzn-drivers). It makes the ENA
Page Pool allocate order-2 compound pages so one 16 KiB allocation can back a
jumbo RX descriptor. This is an experimental optimization, not a correctness
requirement or an upstream ENA interface.

The patch requires:

- a 4 KiB base page size (`getconf PAGESIZE` must print `4096`);
- no XDP or AF_XDP on the patched ENA interfaces;
- `large_rx_page=1` when the module is loaded; and
- TCP receive autotuning without an application-set `SO_RCVBUF`. Compound-page
  SKB truesize accounting can otherwise constrain a fixed receive buffer.

An out-of-tree module is specific to the kernel against which it was built.
After a kernel update, rebuild and install it for the new kernel before
rebooting. On a remote instance, retain a recovery path before replacing the
driver that provides its network access. Secure Boot also requires the module
to be signed by a trusted key or Secure Boot to be disabled.

Run the common checkout, patch, and build steps from this directory:

```bash
reference_dir=$(pwd -P)
kernel_release=$(uname -r)
driver_dir="$PWD/amzn-drivers"
patch_file="$reference_dir/ena-large-rx-page-order2.patch"
if [[ ! -f $patch_file ]]; then
  patch_file="$reference_dir/../ena-large-rx-page-order2.patch"
fi
test -f "$patch_file"

git clone https://github.com/amzn/amzn-drivers.git "$driver_dir"
git -C "$driver_dir" checkout ena_linux_2.17.2
git -C "$driver_dir" apply --check "$patch_file"
git -C "$driver_dir" apply "$patch_file"
make -C "$driver_dir/kernel/linux/ena" -j"$(nproc)"

sudo install -D -m 0644 \
  "$driver_dir/kernel/linux/ena/ena.ko" \
  "/lib/modules/$kernel_release/updates/ena.ko"
sudo depmod -a "$kernel_release"

printf '%s\n' 'options ena large_rx_page=1' | \
  sudo tee /etc/modprobe.d/ena-large-rx-page.conf >/dev/null
modprobe -c | grep '^options ena large_rx_page=1$'
sudo modinfo -k "$kernel_release" -n ena
```

The last command must resolve to the installed `updates/ena.ko`, not the
distribution's in-tree copy.

### Ubuntu

Install build prerequisites before the common steps:

```bash
sudo apt-get update
sudo apt-get install -y git make gcc "linux-headers-$(uname -r)"
```

After the common steps, rebuild the initramfs for the same running kernel and
verify that the module option was included:

```bash
kernel_release=$(uname -r)
sudo update-initramfs -u -k "$kernel_release"
sudo lsinitramfs "/boot/initrd.img-$kernel_release" | \
  grep -E 'updates/ena\.ko|ena-large-rx-page\.conf'
sudo reboot
```

`update-initramfs` is the Ubuntu command; `update-initrd` is not.

### Amazon Linux 2023

Install build prerequisites before the common steps:

```bash
sudo dnf install -y git make gcc "kernel-devel-$(uname -r)"
```

Amazon Linux 2023 builds its initramfs with dracut. A custom file under
`/etc/modprobe.d` was not reliably copied into the image by the distribution's
generic/host-only configuration, so explicitly include both the ENA module and
its option file:

```bash
printf '%s\n' \
  'add_drivers+=" ena "' \
  'install_items+=" /etc/modprobe.d/ena-large-rx-page.conf "' | \
  sudo tee /etc/dracut.conf.d/90-ena-large-rx-page.conf >/dev/null

kernel_release=$(uname -r)
sudo dracut --force --kver "$kernel_release"

sudo lsinitrd -k "$kernel_release" \
  -f /etc/modprobe.d/ena-large-rx-page.conf
sudo lsinitrd -k "$kernel_release" | \
  grep -E 'updates/ena\.ko|ena-large-rx-page\.conf'
sudo reboot
```

The first `lsinitrd` command must print
`options ena large_rx_page=1`; the second must show both the option file and
`updates/ena.ko`.

## Verify after reboot

Replace `enp135s0` if the first profile NIC has another name:

```bash
test "$(getconf PAGESIZE)" = 4096
modinfo -n ena
modinfo -F version ena
cat /sys/module/ena/parameters/large_rx_page
ethtool -i enp135s0
ethtool -S enp135s0 | grep large_rx
```

Expected results include:

- `modinfo -n ena` points into `/lib/modules/$(uname -r)/updates/`;
- the driver version is `2.17.2g`;
- `large_rx_page` is `Y`; and
- the `large_rx_*` statistics are present (with page order `2` once queues are
  initialized).

## Run the reference

Supply a region-specific list of jumbo-capable S3 frontend IPs. To export a
fresh, suffix-filtered catalog with the instance role and run it:

```bash
ENDPOINT_IP_FILE="$PWD/jumbo-us-east-2.txt" \
CATALOG_S3_URI='s3://rapids-tpch/tpch-rs/scale-1000/' \
CATALOG_SUFFIX='.parquet' \
CATALOG_JSON="$PWD/rapids-tpch-scale-1000.json" \
./run_s3_ktls_cuda_ref.sh
```

For the receive/kTLS ceiling without H2D submissions, add
`PAYLOAD_SINK=receive-only`. Generated binaries, snapshots, and results go to
`OUT_DIR` (the invocation directory by default); the tracked CUDA source stays
in this directory.
