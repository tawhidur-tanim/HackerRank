;with rankedSubs
as
(
	select Submission_Date, Hacker_ID, DENSE_RANK() over (order by Submission_date) DateRank
	from Submissions
)

select 
 details.Submission_Date, UniqueSub, maxSub.Hacker_ID, Hackers.Name
from (

select Submission_Date, count(hacker_Id)UniqueSub from 
(
		select distinct
		Submission_Date
		,Hacker_ID
		, (
			
			select count(distinct Submission_Date)
			from Submissions sb
			where sb.Hacker_ID = rb.Hacker_ID
			and sb.Submission_Date <= rb.Submission_Date
		
		)UntilSub
		,DateRank
		from 
		rankedSubs rb
	
	)final
	where DateRank = UntilSub
	group by Submission_Date
)details

inner join    
(	select *, ROW_NUMBER()over (partition by submission_date order by subNum desc, hacker_id)maxSub_hacker
	from 
	(
		select distinct Submission_Date, Hacker_id, count(Hacker_Id) over ( partition by Submission_date,Hacker_id) SubNum
		from Submissions
	)maxsub_first
)maxSub

on details.Submission_Date = maxSub.Submission_Date
inner join Hackers on Hackers.Hacker_ID = maxSub.Hacker_ID

where maxSub.maxSub_hacker = 1