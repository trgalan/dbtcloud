select email, count(*) as cnt
from {{ ref('bronze__customers') }}
group by email
having count(*) > 1