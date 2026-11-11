-- An item that cannot be bought and re-sold as bought (seed not_flippable, ADR-0008) keeps its
-- row and Data grade but is never an idea. Rows returned here break the rule.
select m.item_id, m.data_grade, m.net_gp, m.is_idea, m.idea_rank
from {{ ref('market_snapshot') }} m
join {{ ref('not_flippable') }} n using (item_id)
where m.is_idea is not false
   or m.idea_rank is not null
