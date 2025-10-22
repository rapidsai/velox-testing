--TPCH Q20
select
    s.name,
    s.address
from
    supplier AS s,
    nation AS n
where
    s.suppkey in (
        select
            ps.suppkey
        from
            partsupp AS ps
        where
            ps.partkey in (
                select
                    p.partkey
                from
                    part AS p
                where
                    p.name like 'forest%'
            )
          and ps.availqty > (
            select
                0.5 * sum(l.quantity)
            from
                lineitem AS l
            where
                l.partkey = ps.partkey
              and l.suppkey = ps.suppkey
              and l.shipdate >= date '1994-01-01'
              and l.shipdate < date '1994-01-01' + interval '1' year
        )
    )
  and s.nationkey = n.nationkey
  and n.name = 'CANADA'
order by
    s.name;
