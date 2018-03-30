SET automatview.training_sample_count = 1;

select
	c_custkey,
	c_name,
	sum(l_extendedprice),
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer join orders on c_custkey = o_custkey
	join lineitem on o_orderkey = l_orderkey
	join nation on c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment;

select
	c_custkey,
	c_name,
	sum(l_extendedprice),
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer join orders on c_custkey = o_custkey
	join lineitem on o_orderkey = l_orderkey
	join nation on c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment;
