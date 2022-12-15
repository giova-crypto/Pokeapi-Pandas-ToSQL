use tgb;
select p.id,p.name, p.image  
from ptypes as pt 
join pokemons as p on pt.pokemon_id = p.id 
group by p.id having COUNT(*) >1
AND p.id > 0;