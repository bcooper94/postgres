drop materialized view if exists person_avg_age;

drop table if exists person;
create table person (
    name varchar(255) not null,
    age int not null,
    id serial primary key
);

drop table if exists employee;
create table employee (
    id serial primary key,
    person_id integer,
    foreign key (person_id) references person (id) 
);

drop table if exists manager;
create table manager (
    id serial primary key,
    employee_id integer,
    foreign key (employee_id) references employee (id)
);

drop table if exists executive;
create table executive (
    id serial primary key,
    manager_id integer,
    foreign key (manager_id) references manager (id)
);

insert into person(name, age) values ('Bob', 29), ('Asdf', 41), ('Afdserew', 12);

create materialized view person_avg_age as select count(*), avg(age) from person; 
create materialized view person_count as select count(*) from person;
--create materialized view person_age_sum as select sum(age) from person;
create materialized view person_name_age as select name, age from person;
