--TPCH Q17
select
    sum(l.extendedprice) / 7.0 as avg_yearly
from
    lineitem AS l,
    part AS p
where
    p.partkey = l.partkey
  and p.brand = 'Brand#23'
  and p.container = 'MED BOX'
  and l.quantity < (
    select
        0.2 * avg(l.quantity)
    from
        lineitem AS l
    where
        l.partkey = p.partkey);
