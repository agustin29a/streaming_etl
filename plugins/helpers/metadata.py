dataframe_metadata = {
        "users": {
            "primary_keys": ["user_id"],
            "foreign_keys": None,
            "if_exists": "replace",
        },
        "profiles": {
            "primary_keys": ["profile_id"],
            "foreign_keys": {"user_id": {"table": "users", "column": "user_id"}},
            "if_exists": "replace",
        },
        "subscription_plans": {
            "primary_keys": ["plan_id"],
            "foreign_keys": None,
            "if_exists": "replace",
        },
        "subscriptions": {
            "primary_keys": ["subscription_id"],
            "foreign_keys": {
                "user_id": {"table": "users", "column": "user_id"},
                "plan_id": {"table": "subscription_plans", "column": "plan_id"},
            },
            "if_exists": "replace",
        },
        "content": {
            "primary_keys": ["content_id"],
            "foreign_keys": None,
            "if_exists": "replace",
        },
        "genres": {
            "primary_keys": ["genre_id"],
            "foreign_keys": None,
            "if_exists": "replace",
        },
        "content_genres": {
            "primary_keys": None,
            "foreign_keys": {
                "content_id": {"table": "content", "column": "content_id"},
                "genre_id": {"table": "genres", "column": "genre_id"},
            },
            "if_exists": "replace",
        },
        "people": {
            "primary_keys": ["person_id"],
            "foreign_keys": None,
            "if_exists": "replace",
        },
        "content_people": {
            "primary_keys": ["content_id", "person_id", "role"],
            "foreign_keys": {
                "content_id": {"table": "content", "column": "content_id"},
                "person_id": {"table": "people", "column": "person_id"},
            },
            "if_exists": "replace",
        },
        "series": {
            "primary_keys": ["series_id"],
            "foreign_keys": {
                "content_id": {"table": "content", "column": "content_id"}
            },
            "if_exists": "replace",
        },
        "seasons": {
            "primary_keys": ["season_id"],
            "foreign_keys": {"series_id": {"table": "series", "column": "series_id"}},
            "if_exists": "replace",
        },
        "episodes": {
            "primary_keys": ["episode_id"],
            "foreign_keys": {"season_id": {"table": "seasons", "column": "season_id"}},
            "if_exists": "replace",
        },
        "plays": {
            "primary_keys": ["play_id"],
            "foreign_keys": {
                "user_id": {"table": "users", "column": "user_id"},
                "content_id": {"table": "content", "column": "content_id"},
                "profile_id": {"table": "profiles", "column": "profile_id"},
            },
            "if_exists": "replace",
        },
        "user_lists": {
            "primary_keys": ["list_id"],
            "foreign_keys": {"user_id": {"table": "users", "column": "user_id"}},
            "if_exists": "replace",
        },
        "user_list_content": {
            "primary_keys": ["list_id", "content_id"],
            "foreign_keys": {
                "list_id": {"table": "user_lists", "column": "list_id"},
                "content_id": {"table": "content", "column": "content_id"},
            },
            "if_exists": "replace",
        },
        "ratings": {
            "primary_keys": ["rating_id"],
            "foreign_keys": {
                "user_id": {"table": "users", "column": "user_id"},
                "content_id": {"table": "content", "column": "content_id"},
            },
            "if_exists": "replace",
        },
        "devices": {
            "primary_keys": ["device_id"],
            "foreign_keys": {"user_id": {"table": "users", "column": "user_id"}},
            "if_exists": "replace",
        },
    }

