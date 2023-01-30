CREATE TABLE IF NOT EXISTS public.artists (
	artistid text NOT NULL,
	name text,
	location text,
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" text,
	songid text,
	artistid text,
	sessionid int4,
	location text,
	user_agent text,
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE IF NOT EXISTS public.songs (
	songid text NOT NULL,
	title text,
	artistid text,
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE IF NOT EXISTS public.staging_events (
	artist text,
	auth text,
	firstname text,
	gender text,
	iteminsession int4,
	lastname text,
	length numeric(18,0),
	"level" text,
	location text,
	"method" text,
	page text,
	registration numeric(18,0),
	sessionid int4,
	song text,
	status int4,
	ts int8,
	useragent text,
	userid int4
);

CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs int4,
	artist_id text,
	artist_name text,
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location text,
	song_id text,
	title text,
	duration numeric(18,0),
	"year" int4
);

CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" text,
	"year" int4,
	weekday text,
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name text,
	last_name text,
	gender text,
	"level" text,
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
