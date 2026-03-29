DROP TABLE IF EXISTS raw.movies;
DROP TABLE IF EXISTS raw.ratings;
DROP TABLE IF EXISTS raw.tags;
DROP TABLE IF EXISTS raw.links;

CREATE TABLE raw.movies (
    movie_id INTEGER,
    title TEXT,
    genres TEXT
);

CREATE TABLE raw.ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating NUMERIC(2,1),
    rating_timestamp BIGINT
);

CREATE TABLE raw.tags (
    user_id INTEGER,
    movie_id INTEGER,
    tag TEXT,
    tag_timestamp BIGINT
);

CREATE TABLE raw.links (
    movie_id INTEGER,
    imdb_id INTEGER,
    tmdb_id INTEGER
);