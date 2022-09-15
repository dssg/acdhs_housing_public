select 
    ch.client_hash, 
    ch.as_of_date, 
    case when extract(month from as_of_date)= 1 then 1 else 0 end month_jan,
    case when extract(month from as_of_date)= 2 then 1 else 0 end month_feb,
    case when extract(month from as_of_date)= 3 then 1 else 0 end month_mar,
    case when extract(month from as_of_date)= 4 then 1 else 0 end month_apr,
    case when extract(month from as_of_date)= 5 then 1 else 0 end month_may,
    case when extract(month from as_of_date)= 6 then 1 else 0 end month_jun,
    case when extract(month from as_of_date)= 7 then 1 else 0 end month_jul,
    case when extract(month from as_of_date)= 8 then 1 else 0 end month_aug,
    case when extract(month from as_of_date)= 9 then 1 else 0 end month_sep,
    case when extract(month from as_of_date)= 10 then 1 else 0 end month_oct,
    case when extract(month from as_of_date)= 11 then 1 else 0 end month_nov,
    case when extract(month from as_of_date)= 12 then 1 else 0 end month_dec
from {cohort} ch