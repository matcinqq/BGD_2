DROP TABLE IF EXISTS clean.movies;
DROP TABLE IF EXISTS clean.ratings;
DROP TABLE IF EXISTS clean.tags;
DROP TABLE IF EXISTS clean.links;

CREATE TABLE clean.movies AS
SELECT
    movie_id,
    TRIM(title) AS title,
    TRIM(genres) AS genres
FROM raw.movies
WHERE movie_id IS NOT NULL;

CREATE TABLE clean.ratings AS
SELECT
    user_id,
    movie_id,
    rating,
    TO_TIMESTAMP(rating_timestamp) AS rating_ts
FROM raw.ratings
WHERE user_id IS NOT NULL
  AND movie_id IS NOT NULL
  AND rating IS NOT NULL;

CREATE TABLE clean.tags AS
SELECT
    user_id,
    movie_id,
    TRIM(tag) AS tag,
    TO_TIMESTAMP(tag_timestamp) AS tag_ts
FROM raw.tags
WHERE user_id IS NOT NULL
  AND movie_id IS NOT NULL
  AND tag IS NOT NULL
  AND LENGTH(TRIM(tag)) > 0;

CREATE TABLE clean.links AS
SELECT
    movie_id,
    imdb_id,
    tmdb_id
FROM raw.links
WHERE movie_id IS NOT NULL;