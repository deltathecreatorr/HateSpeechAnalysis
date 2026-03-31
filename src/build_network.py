import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import leidenalg
import igraph as ig


def extract_words(filepath: str, coverage_ratio: float) -> list[tuple[str, float]]:
    """
    Extracts the most important keywords from the cleaned text in the CSVs 
    
    Arguments:
        filepath -- the path to the CSV file containing the cleaned text
        coverage_ratio -- The ratio of total TF-IDF score coverage to determine how many top keywords to keep (default: 0.5 for 50% coverage), provides adaptability to different datasets.
    Returns:
        A list of tuples containing the most important keywords and their scores
    """
    df = pd.read_csv(filepath, usecols=['cleaned_text'])

    corpus = df['cleaned_text'].fillna('').astype(str).tolist()
    vectoriser = TfidfVectorizer(stop_words='english', max_df=0.3, min_df=5)
    matrix = vectoriser.fit_transform(corpus)

    feature_sums = np.asarray(matrix.sum(axis=0)).flatten()
    feature_names = vectoriser.get_feature_names_out()
    
    sorted_indices = feature_sums.argsort()[::-1]
    sorted_sums = feature_sums[sorted_indices]

    cumulative_sum = np.cumsum(sorted_sums)
    total_sum = cumulative_sum[-1]

    cutoff_index = np.searchsorted(cumulative_sum, total_sum * coverage_ratio)

    top_indices = sorted_indices[:cutoff_index + 1]
    top_keywords = feature_names[top_indices]
    top_scores = feature_sums[top_indices]

    sorted_scores = list(zip(top_keywords, top_scores))
    return sorted_scores

def build_user_network(filepath: str, top_keywords: list, user_col: str = 'username') -> nx.Graph:
    vocab = [word for word, score in top_keywords]

    df = pd.read_csv(filepath, usecols=['cleaned_text', user_col, 'sentiment_score'])

    top_active_users = df[user_col].value_counts().head(1500).index.tolist()
    df = df[df[user_col].isin(top_active_users)]

    user_data = df.groupby(user_col).agg({
        'cleaned_text': lambda x: ' '.join(x),
        'sentiment_score': 'mean'
    }).reset_index()

    users_array = np.asarray(user_data[user_col])
    sentiments = user_data['sentiment_score'].tolist()

    sentiment_dict = {
        users_array[i]: {'sentiment': sentiments[i]} for i in range(len(users_array))
    }

    tfidf = TfidfVectorizer(vocabulary=vocab, stop_words='english')
    matrix = tfidf.fit_transform(user_data['cleaned_text'])

    similarity_matrix = cosine_similarity(matrix)

    best_modularity = -1
    best_threshold = 0.0
    best_percentile = 0

    test_percentiles = np.arange(80, 99.5, 0.5)

    for percentile in test_percentiles:
        upper_indices = np.triu_indices_from(similarity_matrix, k=1)
        threshold = np.percentile(similarity_matrix[upper_indices], percentile)

        temp_graph = nx.Graph()
        rows, cols = np.where(similarity_matrix > threshold)
        mask = rows < cols
        r = rows[mask]
        c = cols[mask]

        valid_edges = list(zip(
            users_array[r],
            users_array[c],
            similarity_matrix[r, c]
        ))

        temp_graph.add_weighted_edges_from(valid_edges)
        temp_graph.remove_nodes_from(list(nx.isolates(temp_graph)))

        if len(temp_graph.nodes) < 10:
            continue

        ig_graph = ig.Graph.from_networkx(temp_graph)
        weights = ig_graph.es['weight']

        partition = leidenalg.find_partition(
            ig_graph,
            leidenalg.ModularityVertexPartition,
            weights=weights,
            seed=42
        )

        if len(partition) > 1:
            modularity = partition.quality()
            if modularity > best_modularity:
                best_modularity = modularity
                best_threshold = threshold
                best_percentile = percentile

    graph = nx.Graph()
    graph.add_nodes_from(users_array)
    nx.set_node_attributes(graph, sentiment_dict)

    rows, cols = np.where(similarity_matrix > best_threshold)
    mask = rows < cols
    r = rows[mask]
    c = cols[mask]

    final_edges = list(zip(
        users_array[r],
        users_array[c],
        similarity_matrix[r, c]
    ))

    graph.add_weighted_edges_from(final_edges)

    isolated = list(nx.isolates(graph))
    graph.remove_nodes_from(isolated)
    print(f"Best Modularity: {best_modularity:.4f} at Threshold: {best_threshold:.4f} (Percentile: {best_percentile}%)")
            
    return graph

def plot_network(graph, event: str):
    pos = nx.spring_layout(graph, k=0.15, iterations=50, seed=42)

    sentiments = [graph.nodes[node].get('sentiment', 0.0) for node in graph.nodes()]

    fig, ax = plt.subplots(figsize=(20, 20))
    fig.patch.set_facecolor('white') 
    ax.set_facecolor('white')

    print("Drawing edges...")
    nx.draw_networkx_edges(
        graph, pos,
        alpha=0.6,       
        width=0.2,        
        edge_color="black",
        ax=ax
    )

    print("Drawing nodes...")
    nodes = nx.draw_networkx_nodes(
        graph, pos,
        node_size=30,
        node_color=sentiments,
        cmap=plt.cm.coolwarm_r,
        vmin=-1.0, 
        vmax=1.0,
        alpha=0.8,
        edgecolors='black',
        linewidths=0.3,
        ax=ax
    )

    cbar = plt.colorbar(nodes, shrink=0.5, pad=0.05, ax=ax)
    cbar.set_label('VADER Sentiment Score (-1 Negative to +1 Positive)', color='black', size=12)
    cbar.ax.yaxis.set_tick_params(color='black')
    plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color='black')

    plt.title(f"{event} - Semantic Echo Chambers", color='black', fontsize=20, pad=20)
    plt.axis('off')

    safe_name = event.replace(" ", "_")
    output_file = f'dataset/{safe_name}_HighRes_Network.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')

    plt.show()

top_words = extract_words('dataset/twitter_Iran_Strikes_EVENT_VADER.csv', coverage_ratio=0.7)

event_graph = build_user_network('dataset/twitter_Iran_Strikes_EVENT_VADER.csv', top_words, user_col='username')
plot_network(event_graph, "Twitter Iran Strikes")