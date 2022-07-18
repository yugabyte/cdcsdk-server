/*
 * Refer: https://www.timescale.com/blog/generating-more-realistic-sample-time-series-data-with-postgresql-generate_series/
 */
CREATE TABLE IF NOT EXISTS host (
	id int PRIMARY KEY,
	host_name TEXT,
	LOCATION jsonb
);

CREATE TABLE IF NOT EXISTS host_data (
	id SERIAL PRIMARY KEY,
    date timestamptz NOT NULL,
	host_id int NOT NULL references host(id),
	cpu double PRECISION,
	tempc int,
	status TEXT	
);

CREATE OR REPLACE FUNCTION random_json(keys TEXT[]='{"a","b","c"}',min_val NUMERIC = 0, max_val NUMERIC = 10) 
   RETURNS JSON AS
$$
DECLARE 
	random_val NUMERIC  = floor(random() * (max_val-min_val) + min_val)::INTEGER;
	random_json JSON = NULL;
BEGIN
	-- again, this adds some randomness into the results. Remove or modify if this
	-- isn't useful for your situation
	if(random_val % 5) > 1 then
		SELECT * INTO random_json FROM (
			SELECT json_object_agg(key, random_between(min_val,max_val)) as json_data
	    		FROM unnest(keys) as u(key)
		) json_val;
	END IF;
	RETURN random_json;
END
$$ LANGUAGE 'plpgsql';


/*
 * Function to create random text, of varying length
 */
CREATE OR REPLACE FUNCTION random_text(min_val INT=0, max_val INT=50) 
   RETURNS text AS
$$
DECLARE 
	word_length NUMERIC  = floor(random() * (max_val-min_val) + min_val)::INTEGER;
	random_word TEXT = '';
BEGIN
	-- only if the word length we get has a remainder after being divided by 5. This gives
	-- some randomness to when words are produced or not. Adjust for your tastes.
	IF(word_length % 5) > 1 THEN
	SELECT * INTO random_word FROM (
		WITH symbols(characters) AS (VALUES ('ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz 0123456789 '))
		SELECT string_agg(substr(characters, (random() * length(characters) + 1) :: INTEGER, 1), ''), 'g1' AS idx
		FROM symbols
		JOIN generate_series(1,word_length) AS word(chr_idx) on 1 = 1 -- word length
		group by idx) a;
	END IF;
	RETURN random_word;
END
$$ LANGUAGE 'plpgsql';

      
/*
 * Function to create a random numeric value between two numbers
 * 
 * NOTICE: We are using the type of 'numeric' in this function in order
 * to visually return values that look like integers (no decimals) and 
 * floats (with decimals). However, if inserted into a table, the assumption
 * is that the appropriate column type is used. The `numeric` type is often
 * not the correct or most efficient type for storing numbers in a table.
 */
CREATE OR REPLACE FUNCTION random_between(min_val numeric, max_val numeric, round_to int=0) 
   RETURNS numeric AS
$$
 DECLARE
 	value NUMERIC = random()* (min_val - max_val) + max_val;
BEGIN
   IF round_to = 0 THEN 
	 RETURN floor(value);
   ELSE 
   	 RETURN round(value,round_to);
   END IF;
END
$$ language 'plpgsql';



