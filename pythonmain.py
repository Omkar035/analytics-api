from fastapi import FastAPI, HTTPException, Body

app = FastAPI()

# Sample data for demonstration purposes
import pymysql
import pymysql.cursors
import math
from .reco import *
from .cohortsurvey import *
#from .graph import *
# https://06.dev.hockeycurve.com/pythonmain?cl_name=Tanishq&a_name=Hiveminds%20India&c_name=Festival%20of%20Diamonds%20Aug24%20Campaign&t_name=all&s_date=2024-08-06&e_date=2024-09-02&co_name=Campaign&spends=yes&sort_column=impressions&sort_ord=desc&page_no=1&page_limit=5

conf = {"database": "hcurve_camp_setting", "user": "hcurve", "password": "hcurve", "host": "127.0.0.1"}

def getTagDetailsmain(t_name,a_name,cl_name,c_name,s_date,e_date,co_name,spends,sort_column,sort_ord,page_no,page_limit):
  print(t_name,a_name,cl_name,c_name,s_date,e_date,co_name,spends,sort_column,sort_ord,page_no,page_limit)
  co_names={'Campaign': 'campaign_name',
         'Device': 'device',
         'Dimension': 'dimension',
         'Geo': 'geo',
         'Lineitems': 'lineitems',
         'Creatives': 'creatives',
         'Template': 'template_data',
         'Time of Day': 'time_of_day',
         'Geo Location': 'geo_bidder',
         'Publisher Bidder': 'publisher_bidder',
         'Publisher': 'publisher',
         'Moment Data': 'moment_data',
         'Targeting': 'targeting_data',
         'User Interaction': 'user_interaction',
         'Events': 'events',
         'Interaction Metrics': 'interaction_events',
         'Video': 'video_data',
         'Match Data': 'match_data',
         'Innings': 'innings_data',
          'Sets':'sets',
          'Geo State':'geo_state',
          'Geo City':'geo_city',
         'Geo State Bidder':'geo_bidder_state',
         'Geo City Bidder': 'geo_bidder_city',
           'Category':'categorical_data',
           'Survey':'survey',
           "Moment Vs Nonmoment":"moment_vs_nonmoment"}
  
  def get_value_from_key(key):
      return co_names.get(key)

  if co_name in co_names:
      co_name = get_value_from_key(co_name)
      
  def get_value_from_key1(key):
      return co_names.get(key)

  if sort_column in co_names:
      sort_column = get_value_from_key1(sort_column)
      
      
  #campaign_join = [campaign.strip() for campaign in c_name.replace("'", "").split(',')]
      

  
  

  # values=[{"t_name":t_name,"a_name":a_name,"cl_name":cl_name,"c_name":c_name,"s_date":s_date,"e_date":e_date,"co_name":co_name,"spends":spends,"sub_cohort":sub_cohort,"first_item":first_item}]
  # return values
  with pymysql.connect(**conf) as conn:
    cur  = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cur1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cur3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
    
    per_page=5
    offset=(int(page_no) - 1) * int(per_page)
   
    if cl_name!="all":
        if c_name=="all":
        
            cur3.execute('''Select SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.Campaign=r.campaign where c.agency=%s AND c.client=%s AND c.date between %s and %s ORDER BY impressions ASC''',[a_name,cl_name,s_date,e_date])
           
            
            
            if cl_name=="demo":
                if t_name != "all" and t_name != None:
                    if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":
                   #campaign all, template not all, cohort is campaign,device,dimension,template_data
                      cur.execute('''select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                    elif co_name=="geo_state" or co_name=="geo_city":
                      cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between {} and {} group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                    elif co_name=="geo_bidder_state" or co_name=="geo_bidder_city":
                      cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between {} and {} group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                    else:
                    #campaign all, template not all, cohort not campaign, device, dimension, template_data, user_interaction
                      cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between {} and {} group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])


                else:
                  if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":
                  #campaign all, template is all, cohort is campaign,device,dimension,template_data
                    cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])



                  elif co_name=="geo_state" or co_name=="geo_city":
                    cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])


                  elif co_name=="geo_bidder_state" or co_name=="geo_bidder_city":
                    cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign in('{}') AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])
                  else:

                #campaign all, template is all, cohort not campaign, device, dimension, template_data, user_interaction
                    cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s and c.client=%s AND c.campaign in('{}') and c.date between %s and %s group by {} ORDER BY {} {} LIMIT {},{}.format()'''.format(co_name,co_name,campaign_join,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])


            else:
              if t_name != "all" and t_name != None:

                if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":

                  cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                elif co_name=="geo_state" or co_name=="geo_city":

                  #campaign all, template not all, cohort is campaign,device,dimension,template_data
                  cur.execute('''select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {}{} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                elif co_name=="geo_bidder_state" or co_name=="geo_bidder_city":

                  #campaign all, template not all, cohort is campaign,device,dimension,template_data
                  cur.execute('''Select count(*) OVER() AS full_count, c.$selectedCohort, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])

                elif co_name=="user_interaction":
                #campaign all, template not all, cohort is user_interaction
                  cur.execute('''Select count(*) OVER() AS full_count, events,sum(user_interaction) as user_interaction from user_interaction where template_data=%s AND agency=%s AND client=%s AND date between %s and %s group by events ORDER BY user_interaction {}LIMIT {},{}'''.format(sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])
                
                elif co_name=="interaction_events":
                  #// campaign all, template not all, cohort is events
                  cur.execute('''Select count(*) OVER() AS full_count, c.events,sum(c.count*r.percent) as count from interaction_events c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.events ORDER BY c.events {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])


                else:
                  #// campaign all, template not all, cohort not campaign, device, dimension, template_data, user_interaction , events
                  cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s and c.client=%s and c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,s_date,e_date])
    #campaign all, template is all, cohort is campaign,device,dimension,template_data
              else:
                if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":


                  cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])
                elif co_name =="geo_state" or co_name =="geo_city":
                  cur.execute('''Select count(*) OVER() AS full_count, c.$selectedCohort, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])

                elif co_name=="geo_bidder_state" or co_name =="geo_bidder_city":
                  cur.execute('''Select count(*) OVER() AS full_count, c.$selectedCohort, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])

                elif co_name =="user_interaction":
                  #// campaign all, template is all, cohort is user_interaction
                  cur.execute('''Select count(*) OVER() AS full_count, events,sum(user_interaction) as user_interaction from user_interaction where agency=%s AND client=%s AND date between %s and %s group by events ORDER BY user_interaction {}LIMIT {},{}'''.format(sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])


                elif co_name =="interaction_events":
                #// campaign all, template is all, cohort is events
                  cur.execute('''Select count(*) OVER() AS full_count,c.events,sum(c.count*r.percent) as count from interaction_events c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.date between %s and %s group by c.events ORDER BY c.count {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[a_name,cl_name,s_date,e_date])


                elif co_name =="geo_bidder" or co_name =="publisher_bidder":
                  cur.execute('''Select count(*) OVER() AS full_count, agency,client,campaign,date,{},sum(imp) as impressions,sum(clicks)as clicks,sum(spends) as spends from {} where agency=%s AND client=%s AND date between %s and %s group by {} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_client,s_date,e_date])

                else:
                  #// campaign all, template is all, cohort not campaign, device, dimension, template_data, user_interaction ,events
                  cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where agency=%s AND client=%s AND date between %s and %s group by {}  ORDER BY {}  {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_client,s_date,e_date])

        else:

          # cur3.execute('''Select SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.Campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s ORDER BY impressions ASC''',[a_name,cl_name,c_name,s_date,e_date])
          
          # cur.execute('''select count(*) OVER() AS full_count,%s, round(sum(impressions*percent)) imps, sum(clicks) clicks from time_of_day where date >= %s and date <= %s and campaign_name=%s order by %s %s LIMIT %s,%s'''.[co_name,s_date,e_date,c_name,sort_column,sort_ord,offset,per_page])
          
          cur.execute('''SELECT COUNT(*) OVER() AS full_count, %s, ROUND(SUM(impressions * percent)) AS impressions, SUM(clicks) AS clicks 
               FROM hcurve_camp_setting.time_of_day 
               WHERE date >= %s AND date <= %s AND campaign_name = %s 
               ORDER BY %s %s 
               LIMIT %s, %s''',
            (co_name, s_date, e_date, c_name, sort_column, sort_ord, offset, per_page))
          

          if t_name != "all" and t_name != None:

            if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":
              #// campaign is not all, template not all, cohort campaign, device, dimension, template_data
              # cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} limit {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])
              # cur.execute('''select count(*) OVER() AS full_count,{}, round(sum(impressions*percent)) imps, sum(clicks) clicks from time_of_day where date >= {} and date <= {} and campaign_name=%s order by {} {} LIMIT {},{}'''.[co_name,s_date,e_date,c_name,sort_column,sort_ord,offset,per_page])
              
              cur.execute('''SELECT COUNT(*) OVER() AS full_count, %s, ROUND(SUM(impressions * percent)) AS imps, 
               SUM(clicks) AS clicks 
               FROM hcurve_camp_setting.time_of_day 
               WHERE date >= %s AND date <= %s AND campaign_name = %s 
               ORDER BY %s %s 
               LIMIT %s, %s''',
            (co_name, s_date, e_date, c_name, sort_column, sort_ord, offset, per_page))
              
            elif co_name =="geo_state" or co_name =="geo_city":
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="geo_bidder_state" or co_name =="geo_bidder_city" :
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign= %s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="user_interaction" :
              #// campaign is not all, template not all, cohort user_interaction
              cur.execute('''Select count(*) OVER() AS full_count, events,sum(user_interaction) as user_interaction from user_interaction where template_data=%s AND agency=%s AND client=%s AND c.campaign= %s AND date between %s and %s group by events ORDER BY user_interaction {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="interaction_events":

              #"Select count(*) OVER() AS full_count,events,sum(count) as count from interaction_events where agency='$agency' AND client='$client' AND campaign='$campaign' AND date between '$startdate' and '$enddate' group by events ORDER BY count $sort_ord LIMIT $offset,$per_page";
            #// campaign is not all, template is all, cohort is events
              cur.execute('''Select count(*) OVER() AS full_count,c.events,sum(c.count*r.percent) as count from interaction_events c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.events ORDER BY c.count {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])



            else:
              #// campaign is not all, template not all, cohort not campaign, device, dimension, template_data, user_interaction ,events
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.template_data=%s AND c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,co_name,sort_column,sort_ord,offset,per_page),[t_name,a_name,cl_name,c_name,s_date,e_date])
          else:
            if co_name=="campaign" or co_name=="device" or co_name=="dimension" or co_name=="template_data":
              #// campaign is not all, template is all, cohort is not campaign, device, dimension, template_data, user_interaction
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="geo_state" or co_name =="geo_city":
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_data c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT  {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="geo_bidder_state" or co_name =="geo_bidder_city" :
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from geo_state_bidder c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])


            elif co_name =="user_interaction":
            #// campaign is not all, template is all, cohort is user_interaction
              cur.execute('''Select count(*) OVER() AS full_count, events,sum(user_interaction) as user_interaction from user_interaction where agency=%s AND client=%s AND campaign=%s AND date between %s and %s  group by events ORDER BY user_interaction {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="interaction_events":

              #"Select count(*) OVER() AS full_count,events,sum(count) as count from interaction_events where agency='$agency' AND client='$client' AND campaign='$campaign' AND date between '$startdate' and '$enddate' group by events ORDER BY count $sort_ord LIMIT $offset,$per_page";
            #// campaign is not all, template is all, cohort is events
              cur.execute('''Select count(*) OVER() AS full_count,c.events,sum(c.count*r.percent) as count from interaction_events c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND c.date between %s and %s group by c.events order by count {} LIMIT {},{}'''.format(sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])

            elif co_name =="geo_bidder" or co_name=="publisher_bidder":
              cur.execute('''Select count(*) OVER() AS full_count, agency,client,campaign,date,{},sum(imp) as impressions,sum(clicks)as clicks,sum(spends) as spends from {} where agency=%s AND client=%s AND campaign=%s AND date between %s and %s group by {} ORDER BY impressions {} LIMIT {},{}'''.format(co_name,co_name,co_name,sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])
              
            # elif co_name=='survey':
            #   cur.execute('''Select date,count(*) OVER() AS full_count, question,option_1,option_2,option_3,option_4,option_5 from survey where agency=%s AND client=%s AND campaign =%s AND date between %s and %s group by question,option_1,option_2,option_3,option_4,option_5 LIMIT {},{}'''.format(offset,per_page),[a_name,cl_name,c_name,s_date,e_date])
            else:
            #// campaign is not all, template is all, cohort is not campaign, device, dimension, template_data, user_interaction, events
              cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from {} c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s AND c.campaign=%s AND date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,cl_name,c_name,s_date,e_date])

    else:
      if co_name=="campaign_name" or co_name=="device" or co_name=="dimension" or co_name=="template_data":
        cur.execute('''select count(*) OVER() AS full_count,c.{}, round(sum(impressions*percent)) imps, sum(clicks) clicks from time_of_day where date >= {} and date <= {} and campaign_name={} order by {} {} LIMIT {},{}'''.format(co_name,s_date,e_date,c_name,sort_column,sort_ord,offset,per_page))
#         cur.execute('''Select count(*) OVER() AS full_count, c.{}, SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s  AND c.date between %s and %s group by c.{} ORDER BY {} {} LIMIT {},{}'''.format(co_name,co_name,sort_column,sort_ord,offset,per_page),[a_name,s_date,e_date])

#         cur3.execute('''Select SUM(c.imp*r.percent)as impressions,sum(c.clicks) as clicks,sum(c.spends) as spends from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.Campaign=r.campaign where c.agency=%s AND c.date between %s and %s ORDER BY impressions ASC ''',[a_name,s_date,e_date])

    
    
   
    output= cur.fetchall()
    print(output)
    
    output3=cur3.fetchall()
    
    for item in output3:
      imp=item['impressions']
      
    totimps=imp
    
                    
    
    recommendation=[]
    
    
    
    
    output2=[]
    out=[]
    # recommendation=[]
    finaloutput=[]
    new_dict=[]
    new_dic=[]
    newdic=[]
    total_impressions = 0
    total_clicks = 0
    total_spends= 0
    total_cpc=0
    total_cpm=0
    total_count=0
    if co_name=="interaction_events":
      
      for user_data in output:
        f_count=user_data['full_count']
        event=user_data['events']
        user_int=user_data['count']
        total_count+=user_int
        #selected_cohort1=user_data[co_name]
        with pymysql.connect(**conf) as conn:
            cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute('''Select SUM(c.imp*r.percent)as impressions from campaign c join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign where c.agency=%s AND c.client=%s and c.campaign=%s AND c.date between %s and %s ORDER BY impressions desc''',[a_name,cl_name,c_name,s_date,e_date])
            tot1=cur.fetchall()
            
              #tot2={"Event Name": "Total Impression","Count":imp,"% of Total": "100%" }
            
            x=[d['impressions'] for d in tot1]
            number=0
            for digit in x:
                tot = number*10 + digit
                
        if(co_name=="device"):
              if(user_data['device']=="mweb"):
                  user_data['device']="Mobile web"
                  
              elif(user_data['device']=="mobile"):
                  user_data['device']="Mobile App"
              else:
                  user_data['device']=user_data['device']
      
        
            
        
        percentage=(user_int/tot*100)
        newdic={'Exception': 'Total Impressions', 'count': "{:,.0f}".format(tot),'% of Total':'100%'}
        
        new_dict = {'Event Name': event, 'count': "{:,.0f}".format(user_int),'% of Total':"{:,.2f}%".format(percentage)}
        output2.append(new_dict)
        
        
        total_impressions+=user_int
      total=["","",""]
      # finaloutput.append(output2)
      out.append(newdic)
      
            
    else:

      for item in output:
          impressions_options = item['impressions']
          clicks_options = item['clicks']
          total_impressions += impressions_options
          total_clicks += clicks_options

          if item['spends'] is None:

              spends_options=0
              total_spends=0

          else:
              spends_options=item['spends']
              total_spends += spends_options
              
             



      count=0
      for data in output:

          f_count=data['full_count']
          impressions_options=data['impressions']
          clicks_options=float(data['clicks'])
          spends_options=data['spends']
          


          if(co_name=="device"):
              if(data['device']=="mweb"):
                  data['device']="Mobile web"
                  
              elif(data['device']=="mobile"):
                  data['device']="Mobile App"
              else:
                  data['device']=data['device']

          if impressions_options == 0:
              ctr=0
          else:
              ctr=(float(clicks_options)/float(impressions_options))*100


          imp_dis=impressions_options/totimps*100;
         
          avg_ctr=round((float(total_clicks)/float(total_impressions))*100,2);


          if total_spends==0 :

              data['spends']=0
              spends_options=data['spends']
              total_spends=0
              cpm_option=0
              cpc_option=0


          else:

                  #data['spends']=data['spends']
              spends_options=data['spends']

  #           spends_options=data['spends']

              
           

              if clicks_options == 0:
                  data['CPC']=0
                  cpc_option=0
              else:
                  data['CPC']=round(data['spends']/clicks_options,2)
                  cpc_option=data['CPC']


              if impressions_options==0:
                  data['CPM']=0
                  cpm_option=0
              else:
                  data['CPM']=round(data['spends']*1000/impressions_options,2)

                  cpm_option=data['CPM']

              if total_clicks==0:
                  total_cpc=0
              else:
                  total_cpc=float(total_spends)/float(total_clicks)

              if total_impressions==0:
                  total_cpm=0
              else:
                  total_cpm=(total_spends*1000)/total_impressions
                  
              if spends_options==0:
                data['CPC']=0
                cpc_option=0
                data['CPM']=0
                cpm_option=0
                
              if total_spends==0:
                total_cpm=0
                total_cpc=0
                  
              

      

  
            
          selected_cohort=data[co_name]
          
#           if co_name == 'device':
#               co_name1 = 'Device'
#           elif co_name == 'campaign':
#               co_name1 = 'Campaign'
#           elif co_name == 'dimension':
#               co_name1 = 'Dimension'
#           elif co_name == 'template_data':
#               co_name1 = 'Template'
#           elif co_name == 'time_of_day':
#               co_name1 = 'Time of Day'
#           elif co_name == 'geo':
#               co_name1 = 'Geo'

        
#           else:
#             co_name1=co_name
          #return selected_cohort
  
            #Create a reverse dictionary
          reverse_co_names = {value: key for key, value in co_names.items()}
        
          # Function to get the original value
          def get_original_value(modified_value):
              return reverse_co_names.get(modified_value)

          if co_name in reverse_co_names:
              co_name1 = get_original_value(co_name)
              
          
          round_imp_dis="{:,.2f}%".format(imp_dis)
          

          recom1=[recom(imp_dis,ctr,avg_ctr,selected_cohort,round_imp_dis)]
          recommendation.append(recom1)
          
  
          if total_spends==0:
            new_dict = {co_name1:selected_cohort, 'impressions': "{:,.0f}".format(impressions_options), 'clicks': "{:,.0f}".format(clicks_options),'ctr':"{:,.2f}%".format(ctr),'ID':"{:,.2f}%".format(imp_dis)}
        
          else:
            new_dict = {co_name1:selected_cohort, 'impressions': "{:,.0f}".format(impressions_options), 'clicks': "{:,.0f}".format(clicks_options),'ctr':"{:,.2f}%".format(ctr),'ID':"{:,.2f}%".format(imp_dis),'Spends':"{:,.0f}".format(spends_options),'CPC':"{:,.2f}".format(cpc_option),'CPM':"{:,.2f}".format(cpm_option)}
          output2.append(new_dict)
          
      if total_spends==0:
        
        total=["Total","{:,.0f}".format(total_impressions),"{:,.0f}".format(total_clicks),"{:,.2f}%".format(avg_ctr),str(round((int(total_impressions)/int(totimps)*100),2))+'%']
        
      # elif total_spends==None and spends=='yes':
      #   total=["Total","{:,.0f}".format(total_impressions),"{:,.0f}".format(total_clicks),"{:,.2f}%".format(avg_ctr),str(round((int(total_impressions)/int(totimps)*100),2))+'%',"{:,.0f}".format(total_spends),"{:,.2f}".format(total_cpc),"{:,.2f}".format(total_cpm)]
    
      else:
        
        total=["Total","{:,.0f}".format(total_impressions),"{:,.0f}".format(total_clicks),"{:,.2f}%".format(avg_ctr),str(round((int(total_impressions)/int(totimps)*100),2))+'%',"{:,.0f}".format(total_spends),"{:,.2f}".format(total_cpc),"{:,.2f}".format(total_cpm)]
   
      #chart
    
    if cl_name!='all':
      if c_name!='all':
        if t_name!='all':
          cur1.execute('''Select c.date, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks from campaign c 
      join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign 
      where 
      c.agency=%s and c.client=%s and c.campaign=%s and c.template_data=%s and c.date between %s and %s group by c.date''',[a_name,cl_name,c_name,t_name,s_date,e_date])
        else:
          
          cur1.execute('''Select c.date, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks from campaign c 
      join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign 
      where 
      c.agency=%s and c.client=%s and c.campaign=%s and c.date between %s and %s group by c.date''',[a_name,cl_name,c_name,s_date,e_date])
          
      
      
      else:
        if t_name!='all':

            cur1.execute('''Select c.date, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks from campaign c 
          join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign 
          where 
          c.agency=%s and c.client=%s and c.template_data=%s and c.date between %s and %s group by c.date''',[a_name,cl_name,t_name,s_date,e_date])

        else:

          cur1.execute('''Select c.date, SUM(c.imp*r.percent) as impressions,sum(c.clicks) as clicks from campaign c 
          join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign 
          where 
          c.agency=%s and c.client=%s and c.date between %s and %s group by c.date''',[a_name,cl_name,s_date,e_date])
    else:
      cur1.execute('''Select c.date, SUM(c.imp*r.percent ) as impressions,sum(c.clicks) as clicks from campaign c 
      join reduction r on MONTH(c.date)=r.month AND YEAR(c.date)=r.year and IF(c.device='mweb', r.device='desktop', c.device=r.device) and c.client=r.client and c.campaign=r.campaign 
      where 
      c.agency=%s and c.date between %s and %s group by c.date''',[a_name,s_date,e_date])
    output1= cur1.fetchall()
    
    chartdata=[]
    for item in output1:
      date=item['date']
      impressions_options=item['impressions']
      clicks_options=item['clicks']
      if impressions_options == 0:
        ctr=0
      else:
        ctr=(clicks_options/impressions_options)*100
      new_dict2 = {"date":date, 'impressions': round(impressions_options), 'clicks': round(clicks_options), 'ctr':"{:,.2f}".format(ctr)}
      chartdata.append(new_dict2)
      


    
    
    cur.close()
    cur1.close()
    #finaloutput.append(newdic)
    finaloutput.append(chartdata)
    finaloutput.append(output2)
    finaloutput.append(f_count)
    finaloutput.append(recommendation)
    finaloutput.append(total)
    #finaloutput.append(newdic)
    if co_name=='interaction_events':
      finaloutput.append(out)
    else:
      finaloutput
      

    
    
    
    return finaloutput
    conn.close()
