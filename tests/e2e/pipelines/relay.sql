INSERT INTO {{ sink('docs', 'orders_out') }}
select id, name, tags, "address.city" as city, _id as source_id
from {{ source('docs', 'orders_in') }}
order by id
