------------------------------------------sqlp-40-Mezinova-ИТОГОВАЯ-------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

create role netocourier with login password 'NetoSQL2022' 

grant connect on database postgres to netocourier

grant usage on schema public to netocourier

grant usage on schema extensions to netocourier

grant usage on schema information_schema to netocourier

grant usage on schema pg_catalog to netocourier

grant all on all tables in schema public to netocourier

grant all on view courier_statistic in schema public to netocourier

grant all on function get_courier to netocourier

grant all on ALL FUNCTIONS IN SCHEMA public to netocourier

grant all on ALL procedures IN SCHEMA public to netocourier

GRANT SELECT ON courier_statistic TO netocourier

create type status_courier as enum ('В очереди', 'Выполняется', 'Выполнено', 'Отменен')

grant usage on type status_courier to netocourier

CREATE EXTENSION tsm_system_rows



/*
account: --список контрагентов
id uuid PK
name varchar --название контрагента
*/

create table account (
		id uuid primary key,
		name varchar(100) not null
)

/*
contact: --список контактов контрагентов
id uuid PK
last_name varchar --фамилия контакта
first_name varchar --имя контакта
account_id uuid FK --id контрагента
*/

create table contact (
		id uuid primary key,
		last_name varchar(30) not null,
		first_name varchar(20) not null,
		account_id uuid not null references account (id) on delete restrict on update cascade,
)

/*
user: --сотрудники
id uuid PK
last_name varchar --фамилия сотрудника
first_name varchar --имя сотрудника
dismissed boolean --уволен или нет, значение по умолчанию "нет"
*/

create table "user" (
		id uuid primary key,
		last_name varchar(30) not null,
		first_name varchar(20) not null,
		dismissed boolean not null default false
)

/*
courier: --данные по заявкам на курьера
id uuid PK
from_place varchar --откуда
where_place varchar --куда
name varchar --название документа
account_id uuid FK --id контрагента
contact_id uuid FK --id контакта 
description text --описание
user_id uuid FK --id сотрудника отправителя
status enum -- статусы 'В очереди', 'Выполняется', 'Выполнено', 'Отменен'. По умолчанию 'В очереди'
created_date date --дата создания заявки, значение по умолчанию now()
*/

create table courier (
		id uuid primary key,
		from_place varchar(250) not null,
		where_place varchar(250) not null,
		name varchar(50) not null,
		account_id uuid not null references account (id) on delete restrict on update cascade,
		contact_id uuid not null references contact (id) on delete restrict on update cascade,
		description text,
		user_id uuid not null references "user" (id) on delete restrict on update cascade,
		status status_courier not null default 'В очереди',
		created_date timestamp not null default now()
		
)


--------------------------------------------------------------------------------------------------------
--6. Для возможности тестирования приложения необходимо реализовать процедуру insert_test_data(value), которая принимает на вход целочисленное значение.
/*Данная процедура должна внести:
value * 1 строк случайных данных в отношение account.
value * 2 строк случайных данных в отношение contact.
value * 1 строк случайных данных в отношение user.
value * 5 строк случайных данных в отношение courier.
*/

select id from account tablesample system_rows(1)

create or replace procedure insert_test_data(value int4) as 
$$
declare 
		p_from varchar(250);
		p_where varchar(250);
		p_doc_name varchar(50);
		p_desc text;
		p_stat text[];
		p_date timestamp;
		p_id uuid;
begin
	for i in 1..value
	loop
		insert into account (id, "name")
		values (extensions.uuid_generate_v4(), (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*4))::int), 100)));
	end loop;
	for i in 1..value*2
	loop
		insert into contact (id, last_name, first_name, account_id)
		values (extensions.uuid_generate_v4(), (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 30)), 
				(select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 20)), (select id from account tablesample system_rows(1)));
	end loop;
	for i in 1..value
	loop
		insert into "user" (id, last_name, first_name, dismissed)
		values (extensions.uuid_generate_v4(), (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 30)),
				(select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 20)), (select random()::int)::boolean);
	end loop;
	for i in 1..value*5
	loop
		p_from = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*10))::int), 250));
		p_where = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*10))::int), 250));
		p_doc_name = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*3))::int), 50));
		p_desc= (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, (random()*33)::int), (random()*10)::int), 300));
		p_stat=(select enum_range(null::status_courier));
		p_date= (select now() - interval '1 day' * round(random() * 120) as timestamp);
		insert into courier (id ,from_place, where_place, name, account_id, contact_id, description, user_id, status, created_date)
		values (extensions.uuid_generate_v4(), p_from, p_where, p_doc_name, (select id from account tablesample system_rows(1)), 
				(select id from contact tablesample system_rows(1)), p_desc, (select id from "user" tablesample system_rows(1)), 
				p_stat[(select ceil(random()*4))]::status_courier, p_date);
	end loop;
end;
$$ language plpgsql;

call insert_test_data(1000)

/*
do $$
declare str text;
begin
	for i in 1..1000
	loop
		str = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*4))::int), 100));
		
		insert into account (id, name)
		values(uuid_generate_v4(), str);
	end loop;
end;
$$ language plpgsql

select * from account

truncate table account cascade

do $$
declare str1 text;
		str2 text;
		r_id uuid;
begin
	for i in 1..1000*2
	loop
		str1 = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 30));
		str2 = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 20));
		
		r_id= (select id from account tablesample bernoulli (1) limit 1);
		
		insert into contact (id, last_name, first_name, account_id)
		values(uuid_generate_v4(), str1, str2, r_id);
	end loop;
end;
$$ language plpgsql

do $$
declare p_last_name varchar(30);
		p_first_name varchar(20);
		r_bool boolean;
begin
	for i in 1..1000
	loop
		p_last_name = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 30));
		p_first_name = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*2))::int), 20));
		
		r_bool= (select random()::int);
		
		insert into courier (id, last_name, first_name, dismissed)
		values(uuid_generate_v4(), p_last_name, p_first_name, r_bool);
	end loop;
end;
$$ language plpgsql


do $$
declare p_from varchar(250);
		p_where varchar(250);
		p_doc_name varchar(50);
		p_desc text;
		r_id_acc uuid;
		r_id_usr uuid;
		r_id_cont uuid;
		p_stat text[];
		p_date timestamp;
begin
	for i in 1..1000*5
	loop
		p_from = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*10))::int), 250));
		p_where = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*10))::int), 250));
		p_doc_name = (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, ceil((random()*33))::int), ceil((random()*3))::int), 50));
		p_desc= (select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, (random()*33)::int), (random()*10)::int), 300));
		r_id_acc= (select id from account tablesample bernoulli (1) limit 1);
		r_id_usr= (select id from "user" tablesample bernoulli (1) limit 1);
		r_id_cont= (select id from contact tablesample bernoulli (1) limit 1);
		p_stat=(select enum_range(null::status_courier));
		p_date= (select now() - interval '1 day' * round(random() * 120) as timestamp);
		
		insert into courier (id ,from_place, where_place, name, account_id, contact_id, description, user_id, status, created_date)
		values(uuid_generate_v4(), p_from, p_where, p_doc_name, r_id_acc, r_id_cont, p_desc, r_id_usr, p_stat[(select ceil(random()*4))]::status_courier, p_date);
	
		--raise notice 'p_stat= %', p_stat[(select ceil(random()*4))];
	end loop;
end;
$$ language plpgsql

select now() - interval '1 day' * round(random() * 120) as timestamp

select left(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя ', 1, (random()*34)::int), (random()*10)::int), 300)

select * from contact

select ceil(random()*4)

select enum_range(null::status_courier)

select id from account tablesample bernoulli (1) limit 1

select random()::int 

select * from "user"

select * from account a 

select * from courier c 
*/

--------------------------------------------------------------------------------------------------------
--7. Необходимо реализовать процедуру erase_test_data(), которая будет удалять тестовые данные из отношений

create or replace procedure erase_test_data() as 
$$
begin
	truncate table courier cascade;
	truncate table contact cascade;
	truncate table "user" cascade;
	truncate table account cascade;
end;
$$ language plpgsql;


call erase_test_data()

--------------------------------------------------------------------------------------------------------
--8. Нужно реализовать процедуру add_courier(from_place, where_place, name, account_id, contact_id, description, user_id), 
--которая принимает на вход вышеуказанные аргументы и вносит данные в таблицу courier

create or replace procedure add_courier(p_from_place varchar(250), p_where_place varchar(250), p_name varchar(50), p_account_id uuid, p_contact_id uuid, p_description text, p_user_id uuid) as $$
begin
	insert into courier (id ,from_place, where_place, name, account_id, contact_id, description, user_id)
	values(extensions.uuid_generate_v4(), p_from_place, p_where_place, p_name, p_account_id, p_contact_id, p_description, p_user_id);

end;
$$ language plpgsql;

call add_courier('абвгд', 'абвгдеёжх', 'ааа', '40a3df7f-ed04-4181-b5db-7acfd72cd982', 'fe1b0e82-ee69-4e23-b823-9ee864f28828',null, 'fc8f5d9e-6f5a-4988-b5bb-59aa321ef90e')

/*insert into courier (id ,from_place, where_place, name, account_id, contact_id, user_id)
values(uuid_generate_v4(), 'абвгд', 'абвгдеёжх', 'ааа', 'edca2209-d499-47a2-8e2d-a24f7ad4c27f', '49e443f8-9c5c-4092-812f-c62c56d0ebb0', '97504827-c477-4491-b150-b68684978b5f')

select * from courier c 
order by created_date  desc
*/

--------------------------------------------------------------------------------------------------------
--9. Нужно реализовать функцию get_courier(), которая возвращает таблицу согласно следующей структуры:
/*
id --идентификатор заявки
from_place --откуда
where_place --куда
name --название документа
account_id --идентификатор контрагента
account --название контрагента
contact_id --идентификатор контакта
contact --фамилия и имя контакта через пробел
description --описание
user_id --идентификатор сотрудника
user --фамилия и имя сотрудника через пробел
status --статус заявки
created_date --дата создания заявки
Сортировка результата должна быть сперва по статусу, затем по дате от большего к меньшему.
*/

create or replace function get_courier() 
	returns table (id uuid, from_place varchar(250), where_place varchar(250), name varchar(50), 
					account_id uuid, account varchar(100), contact_id uuid, contact text, 
					description text, user_id uuid, "user" text, status status_courier, created_date timestamp) as $$
begin
	return query
		select cr.id, cr.from_place, cr.where_place, cr.name,
			   cr.account_id, a.name as account, cr.contact_id, concat(c.last_name, ' ', c.first_name) as contact,
			   cr.description , cr.user_id , concat(u.last_name, ' ', u.first_name) as "user", cr.status , cr.created_date 
		from courier cr 
		join account a on cr.account_id =a.id 
		join contact c on cr.contact_id =c.id 
		join "user" u on cr.user_id =u.id 
		order by status, created_date desc;
end;
$$ language plpgsql

select * from get_courier()

--------------------------------------------------------------------------------------------------------
--10. Нужно реализовать процедуру change_status(status, id), которая будет изменять статус заявки. На вход процедура принимает новое значение статуса и значение идентификатора заявки.

create or replace procedure change_status(p_status status_courier, p_id uuid) as $$
begin
	update courier set status=p_status
	where id=p_id;

end;
$$ language plpgsql;

call change_status('Выполняется', '61540344-9c3c-4760-a642-2400222455fe')

/*update courier set status='Выполняется'
where id='1e258626-1405-482a-8275-b6b2865b1134'

select * from courier where id='61540344-9c3c-4760-a642-2400222455fe'

1e258626-1405-482a-8275-b6b2865b1134
18af04a5-39b2-43b3-bd4e-48c4b607920f
*/

--------------------------------------------------------------------------------------------------------
--11. Нужно реализовать функцию get_users(), которая возвращает таблицу согласно следующей структуры:
--user --фамилия и имя сотрудника через пробел 
--Сотрудник должен быть действующим! Сортировка должна быть по фамилии сотрудника.

create or replace function get_users() returns table ("user" text) as $$
begin
	return query
		select concat(u.last_name, ' ', u.first_name) as "user" 
		from "user" u 
		where not u.dismissed
		order by u.last_name;
end;
$$ language plpgsql

select * from get_users()

--------------------------------------------------------------------------------------------------------
--12. Нужно реализовать функцию get_accounts(), которая возвращает таблицу согласно следующей структуры:
--account --название контрагента 
--Сортировка должна быть по названию контрагента.

create or replace function get_accounts() returns table (account varchar(100)) as $$
begin
	return query
		select name 
		from account
		order by name;
end;
$$ language plpgsql

select * from get_accounts()

--------------------------------------------------------------------------------------------------------
--13. Нужно реализовать функцию get_contacts(account_id), которая принимает на вход идентификатор контрагента и возвращает таблицу с контактами переданного контрагента согласно следующей структуры:
--contact --фамилия и имя контакта через пробел 
--Сортировка должна быть по фамилии контакта. Если в функцию вместо идентификатора контрагента передан null, нужно вернуть строку 'Выберите контрагента'.

create or replace function get_contacts(p_account_id uuid) returns table (contact text) as $$
begin
	if p_account_id is null then
		return query
			select 'Выберите контрагента' as contact ;
	else
		return query
			select concat(c.last_name, ' ', c.first_name) as contact 
			from contact c
			where c.account_id=p_account_id
			order by c.last_name;
	end if;
end;
$$ language plpgsql

select * from get_contacts('40a3df7f-ed04-4181-b5db-7acfd72cd982')

select * from get_contacts(null)


--------------------------------------------------------------------------------------------------------
--14. Нужно реализовать представление courier_statistic, со следующей структурой:
/*
account_id --идентификатор контрагента
account --название контрагента
count_courier --количество заказов на курьера для каждого контрагента
count_complete --количество завершенных заказов для каждого контрагента
count_canceled --количество отмененных заказов для каждого контрагента
percent_relative_prev_month -- процентное изменение количества заказов текущего месяца к предыдущему месяцу для каждого контрагента, если получаете деление на 0, то в результат вывести 0.
count_where_place --количество мест доставки для каждого контрагента
count_contact --количество контактов по контрагенту, которым доставляются документы в текущий момент времени! (status=выполняется)
cancel_user_array --массив с идентификаторами сотрудников, по которым были заказы со статусом "Отменен" для каждого контрагента
*/
explain analyze

select * from courier_statistic

drop view courier_statistic

create view courier_statistic as
with cte_courier as (
	select * from courier c --если в cte поместить всю таблицу и каждый раз дергать cte, а не таблицу?
)
select  a.id , 
		a."name" ,
		c1.count_courier ,
		c2.count_complete, 
		c3.count_canceled,
		case when c5.prev_cnt is null then 0. else round((c5.curr_cnt::numeric-c5.prev_cnt::numeric)/c5.prev_cnt::numeric*100, 2) end as percent_relative_prev_month,
		c4.count_where_place,
		c6.count_contact,
		c7.cancel_user_array
from account a 
left join (
		--count_courier --количество заказов на курьера для каждого контрагента
		select account_id, count(*) as count_courier
		from cte_courier c 
		group by account_id-- , user_id  --оставить или убрать??? если на каждого курьера считать, то по одному контрагенту может быть несколько строк с дубликатами по другим агрегатам
		) c1 on a.id = c1.account_id
left join (
		--count_complete --количество завершенных заказов для каждого контрагента
		select account_id, count(*) as count_complete
		from cte_courier c 
		where status='Выполнено'
		group by account_id
		) c2 on a.id = c2.account_id
left join (	
		--count_canceled --количество отмененных заказов для каждого контрагента
		select account_id, count(*) as count_canceled
		from cte_courier c 
		where status='Отменен'
		group by account_id
		) c3 on a.id = c3.account_id
left join (		
		--count_where_place --количество мест доставки для каждого контрагента
		select account_id, count(distinct where_place) as count_where_place
		from cte_courier c 
		group by account_id
		) c4 on a.id = c4.account_id
left join (		
		--count_contact --количество контактов по контрагенту, которым доставляются документы
		select account_id, count(distinct contact_id) as count_contact
		from cte_courier c 
		where c.status='Выполняется'
		group by account_id
		) c6 on a.id = c6.account_id 
left join (
		--percent_relative_prev_month -- процентное изменение количества заказов текущего месяца к предыдущему месяцу для каждого контрагента, если получаете деление на 0, то в результат вывести 0.
		with cte_cnt_mm as (
			select account_id, date_trunc('month', created_date)::date mm, count(*) cnt
			from cte_courier c 
			where created_date between date_trunc('month', now() - interval '1 month')::date and date_trunc('month', now())::date + interval '1 month' - interval '1 day' --берем только текущий и предыдущий месяц
			group by 1,2 
			order by 1,2) 
		select curr.account_id, curr.cnt as curr_cnt, prev.cnt as prev_cnt
		from cte_cnt_mm curr
		join cte_cnt_mm prev on curr.account_id=prev.account_id and prev.mm=curr.mm - interval '1 month'
		) c5 on a.id = c5.account_id 	
left join (
		--cancel_user_array --массив с идентификаторами сотрудников, по которым были заказы со статусом "Отменен" для каждого контрагента
		select account_id, array_agg(user_id) as cancel_user_array
		from cte_courier c 
		where status ='Отменен'
		group by account_id
		) c7 on a.id=c7.account_id
order by a.id



/*

select  a.id , 
		a."name" ,
		case when c5.prev_cnt is null then 0. else round((c5.curr_cnt::numeric-c5.prev_cnt::numeric)/c5.prev_cnt::numeric*100, 2) end as percent_relative_prev_month
from account a 
left join (
		--percent_relative_prev_month -- процентное изменение количества заказов текущего месяца к предыдущему месяцу для каждого контрагента, если получаете деление на 0, то в результат вывести 0.
		with cte_cnt_mm as (
			select account_id, date_trunc('month', created_date)::date mm, count(*) cnt
			from courier c --cte_courier c 
			where created_date between date_trunc('month', now() - interval '1 month')::date and date_trunc('month', now())::date + interval '1 month' - interval '1 day' --берем только текущий и предыдущий месяц
			group by 1,2 
			order by 1,2) 
		select curr.account_id, 
				curr.mm as curr_mm, 
				curr.cnt as curr_cnt,
				prev.mm as prev_mm, 
				prev.cnt as prev_cnt
		from cte_cnt_mm curr
		join cte_cnt_mm prev on curr.account_id=prev.account_id 
			and prev.mm=curr.mm - interval '1 month'
		) c5 on a.id = c5.account_id 	


select * from courier c where account_id ='bbc2a15e-598f-41e8-8cfe-edfd18789765'

select date_trunc('month', now())::date + interval '1 month'-interval '1 day'

--(count(*)-lag(count(*)) over (partition by account_id, date_trunc('month',created_date) order by date_trunc('month',created_date)))/lag(count(*)) over (partition by account_id, date_trunc('month',created_date) order by date_trunc('month',created_date))*100

select date_trunc('month', now()- interval '1 month')::date

select  a.id , 
		a."name" ,
		--user_id,
		count(c.user_id) over (partition by c.account_id, c.user_id) as count_courier,
		case when status ='Выполнено' then count(c.id) over (partition by c.account_id, status) end as count_complete,
		case when status ='Отменен' then count(c.id) over (partition by c.account_id, status) end as count_canceled,
		count(where_place) over (partition by c.account_id) as count_where_place,
		case when status ='Выполняется' then count(c.contact_id) over (partition by c.account_id, status) end as count_contact
from account a 
join courier c on c.account_id =a.id 
order by a.id 


select account_id, array_agg(user_id) as cancel_user_array
from courier c 
where status ='Отменен'
group by account_id



select account_id, 
		array_agg(user_id) as cancel_user_array
from courier c 
where status ='Отменен'
group by account_id



select * from account a 

*/