--TPCH Q11
select
    ps.partkey,
    sum(ps.supplycost * ps.availqty) as value
from
    partsupp AS ps,
    supplier AS s,
    nation AS n
where
    ps.suppkey = s.suppkey
  and s.nationkey = n.nationkey
  and n.name = 'GERMANY'
group by
    ps.partkey having
    sum(ps.supplycost * ps.availqty) > (
    select
    sum(ps.supplycost * ps.availqty) * 0.0001
    from
    partsupp AS ps,
    supplier AS s,
    nation AS n
    where
    ps.suppkey = s.suppkey
                  and s.nationkey = n.nationkey
                  and n.name = 'GERMANY'
    )
order by
    value desc;
