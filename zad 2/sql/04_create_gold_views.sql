DROP VIEW IF EXISTS gold.movie_rating_summary;

CREATE VIEW gold.movie_rating_summary AS
SELECT
    m.movie_id,
    m.title,
    m.genres,
    COUNT(r.user_id) AS rating_count,
    ROUND(AVG(r.rating), 3) AS avg_rating
FROM clean.movies m
JOIN clean.ratings r
    ON m.movie_id = r.movie_id
GROUP BY
    m.movie_id,
    m.title,
    m.genres;