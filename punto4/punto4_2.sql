use tgb;
select pt.name, COUNT(*) from ptypes as pt
group by pt.name
order by COUNT(*) desc
limit 1