select
    ch.client_hash,
    ch.as_of_date,
    {feature_cols}
from
    {cohort} as ch
    left join ({from_obj}) as f
        on ch.client_hash = f.client_hash and f.{knowledge_date} < ch.as_of_date
group by ch.client_hash, ch.as_of_date