import insert_match_schedule as get_schedule
import insert_match_team_dynamic_ranking as get_dyranking
import insert_match_team_score_strength_to_DB as get_strength

def main():
    get_schedule.main()
    get_dyranking.main()
    get_strength.main()

if __name__ == "__main__":
    main()