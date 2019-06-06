CREATE TABLE ftweets
(
    create_at timestamp,
    id bigint NOT NULL,
    text text,
    x float,
    y float,
    in_reply_to_status bigint,
    in_reply_to_user bigint,
    favorite_count bigint,
    retweet_count bigint,
    lang text,
    is_retweet boolean,
    user_id bigint,
    user_name text,
    user_screen_name text,
    user_profile_image_url text,
    user_lang text,
    user_location text,
    user_create_at timestamp,
    user_description text,
    user_followers_count bigint,
    user_friends_count bigint,
    user_statues_count bigint,
    place_country text,
    place_country_code text,
    place_full_name text,
    place_id text,
    place_name text,
    place_place_type text,
    geo_tag_state_id bigint,
    geo_tag_state_name text,
    geo_tag_county_id bigint,
    geo_tag_county_name text,
    geo_tag_city_id bigint,
    geo_tag_city_name text
)
WITH (
    OIDS=FALSE
);

-- Load csv file
COPY ftweets FROM '/tmp/1m-full-tweets.csv' DELIMITER ',' CSV;