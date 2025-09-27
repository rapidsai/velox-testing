--TPCH Q18
select
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice,
    sum(l.quantity)
from
    customer AS c,
    orders AS o,
    lineitem AS l
where
    o.orderkey in (
        select
            l.orderkey
        from
            lineitem AS l
        group by
            l.orderkey having
            sum(l.quantity) > 314
    )
  and c.custkey = o.custkey
  and o.orderkey = l.orderkey
group by
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice
order by
    o.totalprice desc,
    o.orderdate
    limit 100;
