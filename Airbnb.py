#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
import os
from pyspark.sql.types import *


# In[2]:


spark = SparkSession \
    .builder \
    .appName("Airbnb Analysis") \
    .getOrCreate()


# # Calendar dataset transformation

# In[3]:


calendarScehma=StructType([
    StructField('listing_id',IntegerType()),
    StructField('date',DateType()),
    StructField('available',StringType()),
    StructField('price',StringType())
])


# In[ ]:


bucket_name = "yash_airbnb_raw_data"
file = "calendar.csv"

gcs_uri = f"gs://{bucket_name}/{file}"


# In[4]:


calendar=spark.read\
    .format('csv')\
    .option('header','true')\
    .option('sep',',')\
    .schema(calendarScehma)\
    .load(gcs_uri)

#calendar.show()


# In[5]:


calendar_price_clean = calendar.withColumn('price', regexp_replace('price', '\\$', '').cast('int'))


# In[6]:


#calendar_price_clean.count()


# In[7]:


calendar_price_clean=calendar_price_clean.withColumn('month',month('date'))\
                    .withColumn('day',dayofmonth('date'))\
                    .withColumn('year',year('date'))
calendar_clean=calendar_price_clean.dropDuplicates()


# In[8]:


#calendar_clean.show()


# In[9]:


#calendar_clean.select('price')\
         #   .filter(col("listing_id") == 2818).show(100)


# # Listing dataset transformations

# In[10]:


listing_schema="""id int,name string,host_id int,host_name string,neighbourhood_group float,neighbourhood string,latitude float,longitude  float,room_type string,price int,minimum_nights int,number_of_reviews int,last_review date,reviews_per_month float,calculated_host_listings_count int,availability_365 int"""


# In[11]:


bucket_name = "yash_airbnb_raw_data"
file = "listings.csv"

gcs_uri_listing = f"gs://{bucket_name}/{file}"
listing=spark.read\
        .format('csv')\
        .option('header','true')\
        .option('sep',',')\
        .schema(listing_schema)\
        .load(gcs_uri_listing)


# In[12]:


listing=listing.dropDuplicates()


# In[13]:


#listing.show(1)


# In[14]:


#listing.printSchema()


# # Listing details dataset transformation

# In[15]:


listings_details_schema="""id int,listing_url string, scrape_id int, last_scraped date,name string,summary string,space string,description string,experiences_offered string,neighborhood_overview string,
notes string,transit string,access string,interaction string,house_rules string,thumbnail_url float, medium_url float,picture_url string,xl_picture_url float,host_id int,host_url string,
host_name string,host_since date,host_location string,host_about string,host_response_time string,host_response_rate string,host_acceptance_rate int,host_is_superhost string,
host_thumbnail_url string, host_picture_url string, host_neighbourhood  string,host_listings_count float,host_total_listings_count float,host_verifications string,
host_has_profile_pic string, host_identity_verified string, street string, neighbourhood string, neighbourhood_cleansed string, neighbourhood_group_cleansed string, city string,
state string,zipcode string,market string,smart_location string, country_code   string, country string,latitude float,longitude float, is_location_exact string, property_type string, room_type string,
accommodates int,bathrooms float,bedrooms float,beds float,bed_type string,amenities string,square_feet float,price string,
weekly_price string,monthly_price string,security_deposit string,cleaning_fee string,guests_included int,extra_people string,minimum_nights int,maximum_nights int,
calendar_updated string,has_availability string, availability_30 int, availability_60 int, availability_90 int, availability_365 int,calendar_last_scraped date,
number_of_reviews int,first_review date,last_review date,review_scores_rating float, review_scores_accuracy float, review_scores_cleanliness float, review_scores_checkin float, review_scores_communication float,
 review_scores_location float, review_scores_value float,requires_license string, license string, jurisdiction_names string, instant_bookable string, is_business_travel_ready string, cancellation_policy string, require_guest_profile_picture string,
 require_guest_phone_verification string,calculated_host_listings_count int,reviews_per_month float"""


# In[16]:


bucket_name = "yash_airbnb_raw_data"
file = "listings_details.csv"

gcs_uri_listing_details = f"gs://{bucket_name}/{file}"

listing_details=spark.read\
                .format('csv')\
                .option('header','true')\
                .option('sep',',')\
                .schema(listings_details_schema)\
                .load(gcs_uri_listing_details)


# In[17]:


listing_details_clean=listing_details.select('id', 'listing_url', 'scrape_id', 'last_scraped','name', 'summary','space', 
        'description', 'experiences_offered','transit', 'access','house_rules','host_id','host_name','host_response_time', 
        'host_response_rate', 'host_acceptance_rate', 'host_is_superhost','host_neighbourhood', 'host_listings_count',
       'host_total_listings_count', 'host_verifications',
       'host_has_profile_pic', 'host_identity_verified', 'street',
       'neighbourhood', 'neighbourhood_cleansed','city', 'state', 'zipcode', 'market',
       'smart_location', 'country_code', 'country', 'latitude', 'longitude',
       'is_location_exact', 'property_type', 'room_type', 'accommodates',
       'bathrooms', 'bedrooms', 'beds', 'bed_type', 'amenities', 'square_feet',
       'price', 'weekly_price', 'monthly_price', 'security_deposit',
       'cleaning_fee', 'guests_included', 'extra_people', 'minimum_nights',
       'maximum_nights', 'calendar_updated', 'has_availability',
       'availability_30', 'availability_60', 'availability_90',
       'availability_365', 'calendar_last_scraped', 'number_of_reviews',
       'first_review', 'last_review', 'review_scores_rating',
       'review_scores_accuracy', 'review_scores_cleanliness',
       'review_scores_checkin', 'review_scores_communication',
       'review_scores_location', 'review_scores_value', 'requires_license',
       'license', 'jurisdiction_names', 'instant_bookable',
       'is_business_travel_ready', 'cancellation_policy','require_guest_phone_verification',
       'calculated_host_listings_count', 'reviews_per_month').dropDuplicates()


# In[18]:


#listing_details_clean.select(countDistinct("first_review")).show()


# In[19]:


listing_details_df=listing_details_clean.withColumn("price",regexp_replace('price','\\$','').cast('int'))\
                                        .withColumn("weekly_price",regexp_replace('weekly_price','\\$','').cast('int'))\
                                        .withColumn("monthly_price",regexp_replace('monthly_price','\\$','').cast('int'))\
                                        .withColumn("security_deposit",regexp_replace('security_deposit','\\$','').cast('int'))\
                                        .withColumn("cleaning_fee",regexp_replace('cleaning_fee','\\$','').cast('int'))\
                                        .withColumn("extra_people",regexp_replace('extra_people','\\$','').cast('int'))


# In[20]:


#listing_details_df.show(5)
#listing_details_df.printSchema()


# # Neighbourhoods dataset transformations

# In[21]:


neighbourhoods_schema=""" neighbourhood_group float,neighbourhood string"""


# In[22]:


bucket_name = "yash_airbnb_raw_data"
file_path = "neighbourhoods.csv"

gcs_uri_neighbourhoods = f"gs://{bucket_name}/{file_path}"

neighbourhood=spark.read\
                    .format('csv')\
                    .option('header','true')\
                    .option('sep',',')\
                    .schema(neighbourhoods_schema)\
                    .load(gcs_uri_neighbourhoods)


# In[23]:


# neighbourhood.show()


# # Reviews dataset transformations

# In[24]:


reviews_schema=''' listing_id int,date date'''


# In[25]:


bucket_name = "yash_airbnb_raw_data"
file_path = "reviews.csv"

gcs_uri_reviews = f"gs://{bucket_name}/{file_path}"

reviews=spark.read\
            .format('csv')\
            .option('header','true')\
            .option('sep',',')\
            .schema(reviews_schema)\
            .load(gcs_uri_reviews)\
            .dropDuplicates()


# In[26]:


#reviews.show()


# # Reviews_details dataset transformations

# In[27]:


reviews_details_schema= '''listing_id int,id int,date date,reviewer_id int,reviewer_name string,comments string'''


# In[28]:


bucket_name = "yash_airbnb_raw_data"
file_path = "reviews_details.csv"

gcs_uri_reviews_details = f"gs://{bucket_name}/{file_path}"

reviews_details=spark.read\
            .format('csv')\
            .option('header','true')\
            .option('sep',',')\
            .schema(reviews_details_schema)\
            .load(gcs_uri_reviews_details)\
            .dropDuplicates()


# In[29]:


#reviews_details.show()


# # Changing the df to fact and dimensional tables

# In[ ]:


project_id = "airbnb-data-analytics-project"
dataset_id = "airbnb_calendar"


# In[30]:


dimCalendar = calendar_clean\
                .select('listing_id','date','month','day','year','available','price')\
                .withColumn("calendar_id",monotonically_increasing_id())


# In[31]:


dimCalendar=dimCalendar.select('calendar_id','listing_id','date','month','year','day','available','price')


# In[8]:


#dimCalendar.show()
table_name = "dimCalendar"
table_calendar = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimCalendar.write.format("bigquery") \
    .option("table", table_calendar) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# In[33]:


dimReviews1=listing\
                .select('number_of_reviews','last_review','reviews_per_month','id')\
                .withColumn('review_id',monotonically_increasing_id())\
                .withColumnRenamed('id','listing_id')


# In[34]:


dimReviews1=dimReviews1.select('review_id','listing_id','number_of_reviews','last_review','reviews_per_month')


# In[35]:


dimReviews2=listing_details_df.select('id','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin',
                                    'review_scores_communication','review_scores_location')


# In[36]:


dimReviews3=reviews_details.select('id','reviewer_id','reviewer_name','comments','date')\
                            .withColumnRenamed('date','review_date')


# In[37]:


dimReviews=dimReviews1.join(broadcast(dimReviews2),dimReviews1.listing_id == dimReviews2.id,'inner')\
                        .join(broadcast(dimReviews3),dimReviews1.listing_id == dimReviews3.id,'inner')


# In[38]:


dimReviews=dimReviews.select('review_id','listing_id','number_of_reviews','last_review','reviews_per_month','review_scores_rating',
                             'review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication',
                             'review_scores_location','reviewer_id','reviewer_name','comments','review_date')


# In[7]:


#dimReviews.show()
table_name = "dimReviews"
table_reviews = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimReviews.write.format("bigquery") \
    .option("table", table_reviews) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# In[6]:


#dimReviews.printSchema()


# In[44]:


dimListing1=listing.select('id','name','minimum_nights','availability_365')\
                    .withColumn('dimListingid',monotonically_increasing_id())\
                    .withColumnRenamed('id','listing_id')


# In[46]:


dimListing1=dimListing1.select('dimListingid','listing_id','name','availability_365','minimum_nights')


# In[5]:


#dimListing1.show()


# In[62]:


dimListing2=listing_details_df.select('id','property_type','requires_license','jurisdiction_names','instant_bookable','is_business_travel_ready'
                                      ,'cancellation_policy','transit','experiences_offered')\
                                .withColumn('dimListingid1',monotonically_increasing_id())\
                                .withColumnRenamed('id','listing_id1')


# In[65]:


dimListing2=dimListing2.select('dimListingid1','listing_id1','property_type','requires_license','jurisdiction_names','instant_bookable','is_business_travel_ready'
                                      ,'cancellation_policy','transit','experiences_offered')


# In[4]:


#dimListing2.show()


# In[68]:


dimListing=dimListing1.join(broadcast(dimListing2),dimListing1.listing_id == dimListing2.listing_id1,'inner')
                            


# In[73]:


dimListing=dimListing.select('dimListingid','listing_id','name','availability_365','minimum_nights','property_type',
                            'requires_license','jurisdiction_names','instant_bookable','is_business_travel_ready',
                            'cancellation_policy','transit','experiences_offered')


# In[2]:


#dimListing.show()
table_name = "dimListing"
table_listings = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimListing.write.format("bigquery") \
    .option("table", table_listings) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# In[75]:


dimHost=listing_details_df.select('host_id','id','host_name','calculated_host_listings_count','host_is_superhost',
                                  'host_response_time','host_response_rate','host_acceptance_rate',
                                 'host_neighbourhood','host_listings_count','host_total_listings_count',
                                 'host_verifications','host_has_profile_pic','host_identity_verified')\
                            .withColumnRenamed('id','listing_id')


# In[3]:


#dimHost.show()
table_name = "dimHost"
table_hosts = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimHost.write.format("bigquery") \
    .option("table", table_hosts) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# In[77]:


dimRoom=listing_details_df.select('id','room_type','accommodates','bathrooms','bedrooms','beds','bed_type',
                                  'amenities','square_feet')\
                        .withColumnRenamed('id','listing_id')\
                        .withColumn('room_id',monotonically_increasing_id())


# In[79]:


dimRoom=dimRoom.select('room_id','listing_id','room_type','accommodates',
                      'bathrooms','bedrooms','beds','bed_type','amenities','square_feet')


# In[9]:


#dimRoom.show()
table_name = "dimRoom"
table_room = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimRoom.write.format("bigquery") \
    .option("table", table_room) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# In[81]:


dimLocation=listing_details_df.select('id','country','country_code','state','city','street','zipcode','neighbourhood'
                                     ,'market','smart_location','is_location_exact','latitude','longitude')\
                                .withColumnRenamed('id','listing_id')\
                                .withColumn('location_id',monotonically_increasing_id())


# In[83]:


dimLocation=dimLocation.select('location_id','listing_id','country','country_code','state','city','street','zipcode',
                               'neighbourhood'
                                     ,'market','smart_location','is_location_exact','latitude','longitude')


# In[10]:


#dimLocation.show()
table_name = "dimLocation"
table_location = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
dimLocation.write.format("bigquery") \
    .option("table", table_location) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()


# Creating fact table

# In[86]:


dimCalendar.createOrReplaceTempView('dimCalendar')
dimReviews.createOrReplaceTempView('dimReviews')
dimListing.createOrReplaceTempView('dimListing')
dimHost.createOrReplaceTempView('dimHost')
dimRoom.createOrReplaceTempView('dimRoom')
dimLocation.createOrReplaceTempView('dimLocation')
listing_details_df.createOrReplaceTempView('listing_details_df')


# In[90]:


fact_table = spark.sql("""
    SELECT
        dl.listing_id as fact_id,
        dc.calendar_id,
        dr.review_id,
        dh.host_id,
        droom.room_id,
        dl.dimListingid,
        dloc.location_id,
        ldf.price,
        ldf.weekly_price,
        ldf.monthly_price,
        ldf.security_deposit,
        dc.date,
        dr.comments,
        dh.host_name,
        droom.room_type,
        dl.name,
        dloc.city
    FROM
        dimCalendar dc
        JOIN dimReviews dr ON dc.listing_id = dr.listing_id
        JOIN dimHost dh ON dc.listing_id = dh.listing_id
        JOIN dimRoom droom ON dc.listing_id = droom.listing_id
        JOIN dimListing dl ON dc.listing_id = dl.listing_id
        JOIN dimLocation dloc ON dc.listing_id = dloc.listing_id
        JOIN listing_details_df ldf on dc.listing_id=ldf.id
""")


# In[1]:


#fact_table.show()

table_name = "fact_table"
table_fact = f"{project_id}.{dataset_id}.{table_name}"

# Write DataFrame to BigQuery
fact_table.write.format("bigquery") \
    .option("table", table_fact) \
    .option("temporaryGcsBucket", "temp_datproc_cluster") \
    .mode("overwrite") \
    .save()

