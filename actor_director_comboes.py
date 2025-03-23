import pandas as pd
import ast

credits = pd.read_csv("tmdb_5000_credits.csv")
box_office = pd.read_csv("boxoffice_data_2024.csv")

credits.columns = credits.columns.str.lower()
box_office.columns = box_office.columns.str.lower()
box_office["gross"] = box_office["gross"].replace({r'\$':'',',':''}, regex=True)
box_office["gross"] = pd.to_numeric(box_office["gross"],errors="coerce")

def parse_rwo(row):
    cast = ast.literal_eval(row["cast"])
    crew = ast.literal_eval(row["crew"])

#get dirs and actors
    directors = [person["name"] for person in crew if person["job"] == "Director"]
    actors = [person["name"] for person in cast]

    return [(director,actor,row["title"]) for director in directors for actor in actors]

pairs = credits.apply(parse_rwo,axis=1).explode()

df = pd.DataFrame(pairs.tolist(),columns=["director","actor","title"])
#works

merged = pd.merge(df,box_office, on="title")



#grouped = merged.groupby(["director","actor"]).agg(avg_revenue=("gross","mean")).reset_index().sort_values(by="avg_revenue",ascending=False)
grouped= merged.groupby(["director","actor"]).agg(net_revenue=("gross","sum")).reset_index().sort_values(by="net_revenue",ascending=False)


print(grouped.head(30))


