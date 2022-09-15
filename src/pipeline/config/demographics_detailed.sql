--generating demographic features
with parent_child as(
	select
		distinct on (hashed_mci_uniq_id)
		hashed_mci_uniq_id  as client_hash,
		sum(case when pc.relationship ='Child' or pc.relationship ='Daughter' or pc.relationship ='Son' then 1 else 0 end) over (partition by pc.hashed_mci_uniq_id) as num_biological_child,
		sum(case when pc.relationship ='Foster Child' then 1 else 0 end) over (partition by pc.hashed_mci_uniq_id) as num_foster_child,
		sum(case when pc.relationship ='Other Youth Under 18' or pc.relationship ='Other Youth' then 1 else 0 end) over (partition by pc.hashed_mci_uniq_id) as num_other_child,
		count(pc.hashed_mci_uniq_child) over (partition by pc.hashed_mci_uniq_id) as total_num_children_in_household
	from clean.parent_child pc 
),
	cf as(
	select
		distinct on (client_hash)
		*
	from clean.client_feed )
select
    cohort.client_hash,
    cohort.as_of_date,
    case when dob is null then 1 else 0 end age_impute,
    case when dob is null
    	then (   select AVG(DATE_PART('year', cohort.as_of_date::date) - DATE_PART('year', dob))
   	from (select distinct on (client_hash) * from {cohort} as cohort ) as cohort
   	left join clean.client_feed cf using(client_hash))
    	else DATE_PART('year', cohort.as_of_date::date) - DATE_PART('year', dob) end age,
	case when gender in ('1~Male') then 1 else 0 end is_male,
	case when gender in ('2~Female') then 1 else 0 end is_female,
	case when gender in ('99~Unknown') then 1 else 0 end is_unknown,
	case when gender is null then 1 else 0 end gender_impute,
	case when ethnic_desc in ('Hispanic') then 1 else 0 end is_hispanic,
	case when ethnic_desc is null then 1 else 0 end ethnic_desc_impute,
    case when race in ('White')
				then 1 else 0 end is_white,
	case when race in ('Other', 'Asian', 'Two or More Races', 'Other Single Race', 
				'Native Hawaiian/Pacific Islander') then 1 else 0 end is_other_race,
	case when race in ('No Data') then 1 else 0 end no_race_data,			
 	case when race in 
				('Black/African American')
				then 1 else 0 end is_black ,
	case when race is null then 1 else 0 end race_impute,
	case when empt_sts_common_desc in 
				('1~Employed - Full time', '6~Self-employed - Full time') then
				1 else 0 end emp_full_time,
	case when empt_sts_common_desc in 
				('11~Unemployed - Incarcerated') then
				1 else 0 end unemployed_jail,
	case when empt_sts_common_desc in 
				('12~Unemployed - Institutionalized') then
				1 else 0 end unemployed_institution,	
	case when empt_sts_common_desc in 
				('3~Employed - Part time (21 or more hours/week)', '4~Employed - Temporary') then
				1 else 0 end emp_part_time,	
	case when empt_sts_common_desc is null then 1 else 0 end empt_sts_impute,
	case when martl_sts_common_desc in 
				('2~Married', '6~In Civil Union') then
				1 else 0 end is_married,
	case when martl_sts_common_desc is null then 1 else 0 end martl_sts_impute,			
	case when ed_lvl_common_desc in 
			('UNDERGRAD~Some College', 'GRAD COMP~College Degree', 'GRAD~Graduate Degree') then
		1 else 0 end at_least_some_college, 
	case when ed_lvl_common_desc in 
			('9-12~High School (grade 9-12)', 'GRAD~Graduate Degree', 'GED~High School Diploma/GED Completed') then
		1 else 0 end some_high_school, 
	case when ed_lvl_common_desc is null then 1 else 0 end ed_lvl_impute,
	case when total_num_children_in_household is null then 1 else 0 end child_imputation,
	case when total_num_children_in_household is null then 0 else total_num_children_in_household end total_num_children_in_household,
	case when num_biological_child is null then 0 else num_biological_child end num_biological_child,
	case when num_foster_child is null then 0 else num_foster_child end num_foster_child,
	case when num_other_child is null then 0 else num_foster_child end num_other_child	
	-- case when std_zip is null then 0 end std_zip_imputed
	-- TODO: need to figure out how to handle address,zip, etc. 
from 
	{cohort}  as cohort 
   	left join cf using(client_hash)
   	left join parent_child using (client_hash)