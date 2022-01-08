import numpy as np
import pandas as pd



def sample_providers(K,node_variables,active_providers):
      provider_records = node_variables[node_variables["pub_key"].isin(active_providers)]
      nodes = list(provider_records["pub_key"])
      probas = list(provider_records["degree"] / provider_records["degree"].sum())
      return np.random.choice(nodes, size=K, replace=True, p=probas)
    
    
    
def generate_transactions(amount_in_satoshi, K, node_variables,epsilon,active_providers,verbose=False):
      nodes = list(node_variables['pub_key'])
      src_selected = np.random.choice(nodes, size=K, replace=True)
      if epsilon > 0:
          n_prov = int(epsilon*K)
          trg_providers = sample_providers(n_prov,node_variables,active_providers)
          trg_rnd = np.random.choice(nodes, size=K-n_prov, replace=True)
          trg_selected = np.concatenate((trg_providers,trg_rnd))
          np.random.shuffle(trg_selected)
      else:
          trg_selected = np.random.choice(nodes, size=K, replace=True)

      transactions = pd.DataFrame(list(zip(src_selected, trg_selected)), columns=["src","trg"])
      transactions["amount_SAT"] = amount_in_satoshi
      transactions["transaction_id"] = transactions.index
      transactions = transactions[transactions["src"] != transactions["trg"]]
      if verbose:
          print("Number of loop transactions (removed):", K-len(transactions))
          print("Merchant target ratio:", len(transactions[transactions["target"].isin(active_providers)]) / len(transactions))
      return transactions[["transaction_id","src","trg","amount_SAT"]]
    
