-----------------------------------------------
	--ELENA COHORT CREATION -- 
-----------------------------------------------

--------------------Filter for sessions after 4th Jan 2023----------
WITH latest_sessions_2023 AS 
(
  SELECT *
  FROM sessions
  where session_start > '2023-01-04'
),
--------Filter users who have more than or equal to 8 sessions--------
user_filter AS
(
  SELECT user_id,
  COUNT(*)
  FROM latest_sessions_2023
  GROUP BY user_id
  HAVING COUNT(*) >=8
),
-------------------Combined tables------------------------------------
session_combined_table AS
(
  SELECT 
  	ls_23.session_id,
  	ls_23.user_id,
  	ls_23.trip_id,
  	ls_23.session_start,
  	ls_23.session_end,
  	ls_23.flight_discount,
  	ls_23.hotel_discount,
  	ls_23.flight_discount_amount,
  	ls_23.hotel_discount_amount,
  	ls_23.flight_booked,
  	CASE WHEN ls_23.flight_booked = 'yes' THEN 1 ELSE 0 END AS flights_booking_int,
  	ls_23.hotel_booked,
  	CASE WHEN ls_23.hotel_booked = 'yes' THEN 1 ELSE 0 END AS hotel_booking_int,
  	ls_23.page_clicks,
  	ls_23.cancellation,
  	CASE WHEN ls_23.cancellation = 'yes' THEN 1 ELSE 0 END AS cancellation_int,
  	u.birthdate,
  	EXTRACT(Year FROM CURRENT_DATE) - EXTRACT(Year FROM u.birthdate) AS customer_age,
  	u.gender,
  	u.married,
  	u.has_children,
  	u.home_country,
  	u.home_city,
  	u.home_airport,
  	u.home_airport_lat,
  	u.home_airport_lon,
  	u.sign_up_date,
  	f.origin_airport,
  	f.destination,
  	f.destination_airport,
  	f.seats,
  	f.return_flight_booked,
  	f.departure_time,
  	f.return_time,
  	f.checked_bags,
  	f.trip_airline,
  	f.destination_airport_lat,
  	f.destination_airport_lon,
  	f.base_fare_usd,
  	COALESCE(haversine_distance(home_airport_lat,home_airport_lon, destination_airport_lat, destination_airport_lon),0) AS flown_flight_distance,
  	SPLIT_PART(hotel_name, '-', 1) AS Hotel_name,
  	SPLIT_PART(hotel_name, '-', 2) AS Hotel_location,
  	CASE	
  		WHEN h.nights < 0 THEN ABS (h.nights)
  		WHEN h.nights = 0 THEN 1
  		ELSE h.nights
  	END AS nights,	
  	h.rooms,
  	h.check_in_time,
  	h.check_out_time,
  	h.hotel_per_room_usd
  FROM latest_sessions_2023 ls_23
  LEFT JOIN users u
  	ON ls_23.user_id = u.user_id
  LEFT JOIN flights f
  	ON ls_23.trip_id = f.trip_id
  LEFT JOIN hotels h
  	ON ls_23.trip_id = h.trip_id
  WHERE ls_23.user_id IN (SELECT user_id FROM user_filter)
), 
-------------------Discount aggregations------------------------------------
User_discount_agg_metric AS
( 
	SELECT 
		user_id, customer_age, gender, married, has_children, home_city,home_country,
  	COUNT(DISTINCT trip_id) AS num_trips,
  	COUNT(session_id) AS num_sessions,
  	MIN(session_start::DATE) AS user_start_date,
  	MAX(session_end::DATE) AS user_end_date,
  	SUM(CASE WHEN cancellation = True THEN 1 ELSE 0 END) AS num_cancellation,
  	SUM(CASE WHEN flight_discount = True THEN 1 ELSE 0 END) AS num_flights_discount,
  	SUM(CASE WHEN hotel_discount = True THEN 1 ELSE 0 END) AS num_hotel_discount,
  	SUM(COALESCE(hotel_per_room_usd,0)*COALESCE(hotel_discount_amount,0)*COALESCE(rooms,0)*COALESCE(nights,0)) Total_Hotel_Discount_Charges,
  	SUM(COALESCE(base_fare_usd,0)*COALESCE(flight_discount_amount,0)*COALESCE(seats,0)) Total_Flight_Discount_Charges,
  	TO_CHAR(sign_up_date, 'YYYY-mm') AS monthly_sign_up,
  	SUM(COALESCE(base_fare_usd,0) * COALESCE(1-flight_discount_amount,1) * COALESCE(seats,0)) AS Total_Flight_Charges,
  	SUM(COALESCE(hotel_per_room_usd,0) * COALESCE(1-hotel_discount_amount,1) * COALESCE(rooms,0) * COALESCE(nights,0)) AS Total_Hotel_Charges,
  	SUM(COALESCE(base_fare_usd,0) * COALESCE(1-flight_discount_amount,1) * COALESCE(seats,0)) + 
  	SUM(COALESCE(hotel_per_room_usd,0) * COALESCE(1-hotel_discount_amount,1) * COALESCE(rooms,0) * COALESCE(nights,0)) AS Total_Sales
  	FROM session_combined_table
  GROUP BY 
  user_id, customer_age, gender, married, has_children, home_city, home_country, monthly_sign_up
),
-------------------Customer value metrics------------------------------------
Customer_value AS(
SELECT user_id,
  CASE 
 		WHEN customer_age BETWEEN 18 AND 24 THEN 'Student'
  	WHEN customer_age BETWEEN 25 AND 34 THEN 'Young age'
  	WHEN customer_age BETWEEN 35 AND 60 THEN 'Middle age'
  	WHEN customer_age > 60 THEN 'Senior Citizen'
  	ELSE 'Unknown'
  	END AS Age_group,
  ROUND(AVG(Total_Sales)::NUMERIC,2) AS Avg_Total_Sale,
  ROUND((AVG(num_trips) * AVG(Total_Sales))::NUMERIC,2) AS Customer_Value_trip,
  ROUND((AVG(num_sessions) * AVG(Total_Sales))::NUMERIC,2) AS Customer_Value_session,
  ROUND(AVG('2023-07-29' - user_start_date)/180,2) AS avg_cust_lifespan
FROM User_discount_agg_metric
GROUP BY user_id, customer_age
),
--------------------Merge the above two CTE's-------------------------------
Combined_Metric AS
(
  SELECT *
	FROM User_discount_agg_metric
	JOIN Customer_value
  USING(user_id)
),
-----------------Discount Proportion calculations------------------------
Discount_propn AS
(
  SELECT user_id, 
  SUM(CASE WHEN flight_discount THEN 1 ELSE 0 END) AS num_flight_dis_avl,
	ROUND((SUM(CASE WHEN flight_discount THEN 1 ELSE 0 END)::FLOAT / COUNT(*))::NUMERIC, 2) AS flight_discount_proportion,
  ROUND((SUM(CASE WHEN hotel_discount THEN 1 ELSE 0 END)::FLOAT / COUNT(*))::NUMERIC, 2) AS hotel_discount_proportion,
  ROUND(((SUM(CASE WHEN flight_discount THEN 1 ELSE 0 END)+SUM(CASE WHEN hotel_discount THEN 1 ELSE 0 END)) :: FLOAT / COUNT(*))::NUMERIC,2) AS total_discount_proportion,
  COALESCE(ROUND(AVG(flight_discount_amount)::NUMERIC,2),0) AS avg_flight_discount_amount,
  COALESCE(ROUND(AVG(flight_discount_amount*base_fare_usd)::NUMERIC,2),0) AS ADS,
  COALESCE(ROUND(SUM(flight_discount_amount*base_fare_usd)/SUM(flown_flight_distance)::NUMERIC,3),0) AS ADS_per_km
	FROM session_combined_table
	GROUP BY user_id
),

Session_base_final_table AS(
  SELECT user_id,
  	COUNT(DISTINCT (trip_id)) AS bookings_count,
		COUNT(session_id) AS session_count,
  	ROUND((COUNT(DISTINCT (trip_id)) *1.0) / COUNT(session_id),2) AS Booking_rate,
  	SUM(cancellation_int) AS Num_cancellations,
  	SUM(flights_booking_int) - SUM(cancellation_int) AS num_flight_booked,
  	SUM(hotel_booking_int) - SUM(cancellation_int) AS num_hotel_booked,
  	COALESCE(SUM(flown_flight_distance),0) AS total_dist_flown,
  	COALESCE(AVG(flown_flight_distance),0) AS avg_dist_flown_incl, --include values even for cancelled trip
  	COALESCE(ROUND(SUM(nights)*1.0 / COUNT(DISTINCT trip_id)),0) AS avg_hotel_stay,
  	COALESCE(ROUND(SUM(rooms)*1.0 / COUNT(DISTINCT trip_id)),0) AS avg_hotel_rooms,
  	COALESCE(ROUND(SUM(seats)*1.0 / COUNT(DISTINCT trip_id)),0) AS avg_num_seats,
  	CASE
  		WHEN COUNT(DISTINCT (trip_id)) > 0 THEN 
  		ROUND(SUM(cancellation_int)*1.0 / COUNT(DISTINCT (trip_id)),2) ELSE 0 
  		END AS Cancellation_rate,
  	AVG(session_end - session_start) AS Avg_session_duration,
  	COALESCE(EXTRACT (Days FROM AVG(return_time - departure_time)),0) AS Avg_trip_duration,
  	COALESCE(SUM(checked_bags),0) AS Total_bags_checked,
  	COALESCE(ROUND(SUM(checked_bags) * 1.0 / COUNT(DISTINCT (trip_id)),2),0) AS Avg_bags_checked,
  	ROUND(AVG(departure_time::date - session_start::date),2) AS Travel_lead_time,
  	ROUND(AVG(CASE 
        				WHEN cancellation = TRUE THEN (departure_time::date - session_end::date)
        				ELSE NULL
  						END),2) AS Avg_Cancel_lead_time,
  	EXTRACT(Days FROM (MAX(session_end)- MIN(session_start))) AS active_days,
  	ROUND(AVG(page_clicks),2) as avg_page_clicks,
  	SUM(CASE WHEN flight_discount = True AND trip_id IS NOT NULL THEN 1 ELSE 0 END) AS num_flights_discount_applied,
  	SUM(CASE WHEN hotel_discount = True AND trip_id IS NOT NULL THEN 1 ELSE 0 END) AS num_hotel_discount_applied,
  	SUM(CASE WHEN flight_discount = True AND trip_id IS NULL THEN 1 ELSE 0 END) AS num_flights_discount_offered,
  	SUM(CASE WHEN hotel_discount = True AND trip_id IS NULL THEN 1 ELSE 0 END) AS num_hotel_discount_offered,
  	SUM(COALESCE(hotel_per_room_usd,0)*COALESCE(hotel_discount_amount,0)*COALESCE(rooms,0)*COALESCE(nights,0)) Total_Hotel_Discount_Amount,
  	SUM(COALESCE(base_fare_usd,0)*COALESCE(flight_discount_amount,0)*COALESCE(seats,0)) Total_Flight_Discount_Amount
	FROM session_combined_table 
	GROUP BY user_id
),
Final_Metrics_Table AS
(
  SELECT user_id, 
	(bookings_count / session_count) AS purchase_rate,
 	COALESCE((num_flights_discount_applied + num_hotel_discount_applied) / NULLIF(bookings_count, 0),0) AS discount_purchase_rate,
  (Total_Hotel_Discount_Amount / NULLIF(num_hotel_booked,0)) AS Avg_hotel_discount,
  COALESCE(Total_bags_checked / NULLIF(num_flight_booked,0),0) AS Luggate_Ratio,
 	COALESCE(total_dist_flown / NULLIF(num_flight_booked,0),0) AS Avg_dist_flown,--Will not include cancelled
  NULLIF(num_flight_booked,0) / NULLIF(num_hotel_booked,0) AS Hotel_trip_rate
FROM Session_base_final_table
),
Final_single_user_table AS
(
  SELECT * 
  FROM Final_Metrics_Table
  JOIN Session_base_final_table
  USING(user_id)
),
final_aggregation AS
(
	SELECT *, 
		ROUND(ADS_per_km * flight_discount_proportion * avg_flight_discount_amount::NUMERIC,4) AS bargain_hunter_index,
		ROUND((avg_cust_lifespan * Customer_Value_trip),2) AS customer_lifetime_value
FROM Final_single_user_table
LEFT JOIN Combined_Metric
USING(user_id)
JOIN Discount_propn
USING(user_id)
),
-------------------Assign perks------------------------------------
perks_assignment AS
(
  SELECT 
			*,      
  CASE
  		WHEN num_trips >= 4 AND Avg_dist_flown >=2000 THEN 'Priority Check-in & Boarding'
      WHEN num_trips <= 3 AND discount_purchase_rate >= 0.2 THEN 'Exclusive Discount'
      WHEN Avg_Total_Sale >= 1500 AND Travel_lead_time >= 9 THEN 'Extended Free Cancellation Window'     
  		WHEN num_flight_booked >= 3 AND Total_Bags_checked >1 THEN 'Free checked bag'     
      WHEN avg_cust_lifespan >0.60 AND bookings_count >= 3 THEN 'Complimentary Room Upgrade or Seat Upgrade on Next Booking'
      WHEN active_days >=150 AND Cancellation_rate <= 0.30 THEN 'Extended Booking Flexibility'
      WHEN num_hotel_booked >= 1  AND avg_hotel_stay >= 2 THEN 'Free Hotel meal'
      WHEN avg_page_clicks >= 4 AND purchase_rate < 3 THEN '10% Discount on next booking'
      END AS perks_offered
FROM final_aggregation
)
SELECT * FROM perks_assignment

