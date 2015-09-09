data = load '/user/hadoop/mapreduce/data/proxy_logs/http.log' as (line : chararray); 

transf = foreach data generate (double) REGEX_EXTRACT(line,'.*size="([^"]*)".*',1) as size, REGEX_EXTRACT(line,'.*url=\\"http[s]?:\\/\\/([^ : \\/]+).*',1) as url, (double) REGEX_EXTRACT(line,'.*fullreqtime="([^"]*)".*',1) as time;

filtered = filter transf by not (url matches '.*globant.*');

visits = group filtered by url; 

totals = foreach visits generate group, COUNT(filtered) as num, AVG(filtered.size), AVG(filtered.time);

ordered = order totals by num desc;

top = limit ordered 10;

dump top;

