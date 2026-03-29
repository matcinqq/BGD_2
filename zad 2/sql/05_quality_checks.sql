SELECT COUNT(*) AS raw_movies_count FROM raw.movies;
SELECT COUNT(*) AS raw_ratings_count FROM raw.ratings;
SELECT COUNT(*) AS raw_tags_count FROM raw.tags;
SELECT COUNT(*) AS raw_links_count FROM raw.links;

SELECT COUNT(*) AS clean_movies_count FROM clean.movies;
SELECT COUNT(*) AS clean_ratings_count FROM clean.ratings;
SELECT COUNT(*) AS clean_tags_count FROM clean.tags;
SELECT COUNT(*) AS clean_links_count FROM clean.links;