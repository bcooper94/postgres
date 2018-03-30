SET automatview.training_sample_count = 1;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity),
	sum(l_extendedprice),
	avg(l_quantity),
	avg(l_extendedprice),
	avg(l_discount),
	count(l_returnflag)
from
	lineitem
group by
	l_returnflag,
	l_linestatus;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity),
	sum(l_extendedprice),
	avg(l_quantity),
	avg(l_extendedprice),
	avg(l_discount),
	count(l_returnflag)
from
	lineitem
group by
	l_returnflag,
	l_linestatus;
