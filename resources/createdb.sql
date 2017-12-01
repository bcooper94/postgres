drop materialized view if exists person_avg_age;

drop table if exists person;
create table person (
    id serial primary key,
    name varchar(255) not null,
    age int not null
);

insert into person(name, age) values ('Bob', 29), ('Asdf', 41), ('Afdserew', 12);

create materialized view person_avg_age as select count(*), avg(age) from person; 
create materialized view person_count as select count(*) from person;
--create materialized view person_age_sum as select sum(age) from person;
