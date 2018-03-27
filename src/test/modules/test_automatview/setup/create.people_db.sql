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
