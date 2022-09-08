search_terms = ["housing market OR house prices OR home prices OR price of homes OR price of houses"]
tags = ['real estate']
query = "place_country:US -is:retweet -has:hashtags"
rules = [f"housing market {query}",
    f"house prices {query}",
    f"home prices {query}",
    f"price of homes {query}",
    f"price of houses {query}",]


print(rules[0])