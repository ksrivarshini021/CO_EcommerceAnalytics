SELECT 
	city,COUNT(*) as total_breweries, 
	COUNT(CASE WHEN brewery_type = 'micro' THEN 1 END) as micro_count, 
    COUNT(CASE WHEN brewery_type = 'brewpub' THEN 1 END) as brewpub_count
    
FROM breweries GROUP BY city ORDER BY total_breweries DESC LIMIT 10;

-- future competetion report
SELECT city, name FROM breweries WHERE brewery_type = 'planning' ORDER BY city;

-- percentage discribution
WITH StateTotal AS (
	SELECT COUNT(*) as grand_total FROM breweries)
    
SELECT city, COUNT(*) as city_count, ROUND((COUNT(*) / (SELECT grand_total FROM StateTotal)) * 100, 2)
FROM breweries GROUP BY city ORDER BY city_count DESC;
