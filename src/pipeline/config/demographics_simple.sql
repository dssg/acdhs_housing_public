--generating demographic features

with cf as(
	select distinct on (client_hash) *
	from clean.client_feed )
select
    cohort.client_hash,
    cohort.as_of_date,
    case when dob is null then 1 else 0 end age_imputation,
    case when dob is null
    	then (   select AVG(DATE_PART('year', cohort.as_of_date::date) - DATE_PART('year', dob))
   	from (select distinct on (client_hash) * from {cohort} as cohort ) as cohort
   	left join clean.client_feed cf using(client_hash))
    	else DATE_PART('year', cohort.as_of_date::date) - DATE_PART('year', dob) end age,
	case when gender in ('1~Male') then 1 else 0 end is_male,
	case when gender in ('2~Female') then 1 else 0 end is_female,
	case when gender in ('99~Unknown') then 1 else 0 end is_unknown,
	case when gender is null then 1 else 0 end gender_imputation,
    case when race in ('White')
				then 1 else 0 end is_white,
	case when race in ('Other', 'Asian', 'Two or More Races', 'Other Single Race', 
				'Native Hawaiian/Pacific Islander') then 1 else 0 end is_other_race,
	case when race in ('No Data') then 1 else 0 end no_race_data,			
 	case when race in 
				('Black/African American')
				then 1 else 0 end is_black ,
	case when race is null then 1 else 0 end race_imputation
from 
  	{cohort}  as cohort 
   left join cf using(client_hash)