import argparse
import pandas as pd
import psycopg2
import networkx as nx
from datetime import datetime, timedelta
from networkx.algorithms import approximation
from tqdm import tqdm
import numpy as np
import random

def safe_mean(data):
    if data:
        filtered_data = [x for x in data if x is not None]
        if filtered_data:
            return np.mean(filtered_data)
    return None

def directed_avg_clustering(G, trials=10_000):
    nodes = np.random.choice(G.nodes(), min(trials, len(G.nodes())), replace=False)
    return np.mean(list(nx.clustering(G, nodes).values()))

def process_file(file_name):
    file_path = f'../Data/{file_name}_week_ind_edges.npz'
    output_txt = f'../Data/CC_Analysis_{file_name}.txt'
    output_npz = f'../Data/CC_Analysis_{file_name}.npz'
    
    with np.load(file_path, allow_pickle=True) as data:
        week_ind = data['week_ind']
        edges = data['edges']

    end_date = datetime(2024, 6, 1)
    current_date = datetime(2023, 1, 1)
    total_num_weeks = (end_date - current_date).days // 7
    cutoff_dates = [pd.to_datetime(current_date + timedelta(days=7*i), utc=True) for i in range(total_num_weeks+1)]

    random_cc_by_date = []
    true_cc_by_date = []
    ratio_by_date = []
    densities = []
    densities_random = []
    density_ratios = []
    shortest_paths = []
    shortest_paths_random = []

    G_weekly = nx.DiGraph() # We create this graph for all. 
    # Incase it's not following we reset each week.
    # This leads to duplicated creation in Week 0 for all but following but thats okay.
    for i in range(len(week_ind) - 1):
        if file_name != "following":
            G_weekly = nx.DiGraph()
        
        G_weekly.add_edges_from(edges[week_ind[i]:week_ind[i+1]])
        
        in_degree_sequence = [d for _, d in G_weekly.in_degree()]
        out_degree_sequence = [d for _, d in G_weekly.out_degree()]

        G_random = nx.directed_configuration_model(in_degree_sequence, out_degree_sequence, seed=0)
        G_random = nx.DiGraph(G_random)

        end_of_week = current_date + timedelta(days=7)
        week_count = i + 1
        
        avg_clustering = directed_avg_clustering(G_weekly, trials=10000)
        random_approx_mean = directed_avg_clustering(G_random, trials=10000)

        density = nx.density(G_weekly)
        density_random = nx.density(G_random)
        ratio = avg_clustering / random_approx_mean if random_approx_mean != 0 else 0
        ratio_density = density / density_random if density_random != 0 else 0

        nodes = list(G_weekly.nodes())
        if len(nodes) >= 2:
            sampled_nodes = random.choices(list(nx.nodes(G_weekly)), k=min(50_000, len(nodes) * (len(nodes) - 1)))
            sampled_pairs = [(sampled_nodes[i], sampled_nodes[i+1]) for i in range(0, len(sampled_nodes), 2)]

            sampled_nodes_random = random.choices(list(nx.nodes(G_random)), k=min(50_000, len(nodes) * (len(nodes) - 1)))
            sampled_pairs_random = [(sampled_nodes_random[i], sampled_nodes_random[i+1]) for i in range(0, len(sampled_nodes_random), 2)]

            shortest_path_lengths = []
            shortest_path_lengths_random = []

            #for u, v in tqdm(sampled_pairs):
            for i in range(len(sampled_pairs)):
                u, v = sampled_pairs[i]
                r, s = sampled_pairs_random[i]
                try:
                    sp_length = nx.shortest_path_length(G_weekly, source=u, target=v)
                    shortest_path_lengths.append(sp_length)
                except nx.NetworkXNoPath:
                    shortest_path_lengths.append(None)
                
                try:
                    sp_length_random = nx.shortest_path_length(G_random, source=r, target=s)
                    shortest_path_lengths_random.append(sp_length_random)
                except nx.NetworkXNoPath:
                    shortest_path_lengths_random.append(None)

            avg_shortest_path = safe_mean(shortest_path_lengths)
            avg_shortest_path_random = safe_mean(shortest_path_lengths_random)
        else:
            shortest_path_lengths = []
            shortest_path_lengths_random = []
            avg_shortest_path = None
            avg_shortest_path_random = None

        c_time = datetime.now().strftime("%H:%M:%S")
        print(f"Week {current_date} to {end_of_week} ({week_count}/{total_num_weeks} - {c_time})")

        with open(output_txt, "a") as file:
            file.write(f"{current_date},{end_of_week},{avg_clustering or 0},{random_approx_mean or 0},{ratio or 0},{density or 0},{density_random or 0},{ratio_density or 0},{avg_shortest_path or 'None'},{avg_shortest_path_random or 'None'}\n")

        random_cc_by_date.append(random_approx_mean)
        true_cc_by_date.append(avg_clustering)
        ratio_by_date.append(ratio)
        densities.append(density)
        densities_random.append(density_random)
        density_ratios.append(ratio_density)
        shortest_paths.append(shortest_path_lengths)
        shortest_paths_random.append(shortest_path_lengths_random)

        current_date = end_of_week

    np.savez(output_npz, random_cc_by_date=random_cc_by_date, true_cc_by_date=true_cc_by_date, ratio_by_date=ratio_by_date, densities=densities, densities_random=densities_random, density_ratios=density_ratios, shortest_paths=shortest_paths, shortest_paths_random=shortest_paths_random)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process directed graph data files.")
    parser.add_argument("--file", type=str, required=True, help="Name of the file to process (without extension)")

    args = parser.parse_args()
    process_file(args.file)
