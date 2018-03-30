SET automatview.training_sample_count = 1;

select
	l_orderkey,
	sum(l_extendedprice),
	o_orderdate,
	o_shippriority
from
	customer join orders on c_custkey = o_custkey
	join lineitem on l_orderkey = o_orderkey
group by
	l_orderkey,
	o_orderdate,
	o_shippriority;

select
	l_orderkey,
	sum(l_extendedprice),
	o_orderdate,
	o_shippriority
from
	customer join orders on c_custkey = o_custkey
	join lineitem on l_orderkey = o_orderkey
group by
	l_orderkey,
	o_orderdate,
	o_shippriority;
