 WITH cte1 AS (
         SELECT
            id,
            season,
            week,
            offense,
            defense,
            offense_conference,
            defense_conference,
            home,
            away,
            game_id,
            drive_id,
            drive_number,
            play_number,
            period,
            yard_line,
            yards_to_goal,
            down,
            distance,
            scoring,
            yards_gained,
            play_type,
            play_text,
            ppa,
            offense_score,
            defense_score,
            LAG(ABS(offense_score - defense_score)) OVER (PARTITION BY game_id ORDER BY drive_number, play_number) AS gt
           FROM plays
          WHERE ppa IS NOT NULL
        ), cte2 AS (
         SELECT
            id,
            season,
            week,
            offense,
            defense,
            offense_conference,
            defense_conference,
            home,
            away,
            game_id,
            drive_id,
            drive_number,
            play_number,
            period,
            yard_line,
            yards_to_goal,
            down,
            distance,
            scoring,
            yards_gained,
            play_type,
            play_text,
            ppa,
            offense_score,
            defense_score,
            gt,
                CASE
                    WHEN period = 2 AND gt > 38 OR period = 3 AND gt > 28 OR period = 4 AND gt > 22 THEN true
                    ELSE false
                END AS garbage_time
           FROM cte1
        )
 SELECT
    id,
    season,
    week,
    offense,
    defense,
    offense_conference,
    defense_conference,
    home,
    away,
    game_id,
    drive_id,
    drive_number,
    play_number,
    period,
    yard_line,
    yards_to_goal,
    down,
    distance,
    scoring,
    yards_gained,
    play_type,
    play_text,
    ppa,
    offense_score,
    defense_score,
        CASE
            WHEN
				(offense_conference IN ('ACC', 'Big 12', 'Big Ten', 'SEC') OR offense = 'Notre Dame')
				AND ((defense_conference IN ('ACC', 'Big 12', 'Big Ten', 'SEC') OR defense = 'Notre Dame'))
			THEN true
            ELSE false
        END AS p4_only
   FROM cte2
  WHERE garbage_time = false;