import numpy as np
import networkx as nx
import pandas as pd
import math
import copy
import time
import generating_transactions



#environment has an object of simulator
class simulator():
  def __init__(self,
               src,trg,channel_id,
               active_channels, network_dictionary,
               merchants,
               count,
               amount,
               epsilon,
               node_variables,
               active_providers,
               fixed_transactions = True):
    
    self.src = src
    self.trg = trg
    self.channel_id = channel_id
    self.count = count
    self.amount = amount

    self.merchants = merchants #list of merchants
    self.epsilon = epsilon    #ratio of marchant
    self.node_variables = node_variables
    self.active_providers = active_providers
    self.active_channels = active_channels
    self.network_dictionary = network_dictionary
    self.fixed_transactions = fixed_transactions

    self.graph = self.generate_graph(amount)

    if fixed_transactions : 
      self.transactions = generating_transactions.generate_transactions(amount, count, node_variables, epsilon, active_providers)
    else :
      self.transactions = None
 

 

  def calculate_weight(self,edge,amount): 
    return edge[2] + edge[1]*amount 
    


  def sync_network_dictionary(self):
    for (src,trg) in self.active_channels :
      self.network_dictionary[(src,trg)] = self.active_channels[(src,trg)]
      self.network_dictionary[(trg,src)] = self.active_channels[(trg,src)]
      


  def generate_graph(self, amount):
    self.sync_network_dictionary()
    graph = nx.DiGraph()
    for key in self.network_dictionary :
      val = self.network_dictionary[key]
      if val[0] > amount :
          graph.add_edge(key[0],key[1],weight = val[1]*amount + val[2])
    
    return graph




  def update_graph(self, src, trg):
      src_trg = self.active_channels[(src,trg)]
      src_trg_balance = src_trg[0]
      trg_src = self.active_channels[(trg,src)]
      trg_src_balance = trg_src[0]
      
      if (src_trg_balance <= self.amount) and (self.graph.has_edge(src,trg)):
        self.graph.remove_edge(src,trg)
      elif (src_trg_balance > self.amount) and (not self.graph.has_edge(src,trg)): 
        self.graph.add_edge(src, trg, weight = self.calculate_weight(src_trg, self.amount))
      
      if (trg_src_balance <= self.amount) and (self.graph.has_edge(trg,src)):
        self.graph.remove_edge(trg,src)
      elif (trg_src_balance > self.amount) and (not self.graph.has_edge(trg,src)): 
        self.graph.add_edge(trg, src, weight = self.calculate_weight(trg_src, self.amount))
      
    
  


  def update_active_channels(self, src, trg, transaction_amount):
      self.active_channels[(src,trg)][0] = self.active_channels[(src,trg)][0] - transaction_amount
      self.active_channels[(trg,src)][0] = self.active_channels[(trg,src)][0] + transaction_amount




  def update_network_data(self, path, transaction_amount):
      for i in range(len(path)-1) :
        src = path[i]
        trg = path[i+1]
        if (self.is_active_channel(src, trg)) :
          self.update_active_channels(src,trg,transaction_amount)
          self.update_graph(src, trg)
          
          
            
      
  def is_active_channel(self, src, trg):
    return ((src,trg) in self.active_channels)
        

  def onchain_rebalancing(self,onchain_rebalancing_amount,src,trg,channel_id):
    self.active_channels[(src,trg)][0] += onchain_rebalancing_amount  
    self.active_channels[(src,trg)][3] += onchain_rebalancing_amount   
    self.active_channels[(trg,src)][3] += onchain_rebalancing_amount   
    self.update_graph(src, trg)
                




  def get_path_value(self,nxpath,graph) :
    val = 0 
    for i in range(len(nxpath)-1):
      u,v = nxpath[i],nxpath[i+1]
      weight = graph.get_edge_data(u, v)['weight']
      val += weight
    return val
    



  def set_node_fee(self,src,trg,channel_id,action):
      alpha = action[0]
      beta = action[1]
      self.network_dictionary[(src,trg)][1] = alpha
      self.network_dictionary[(src,trg)][2] = beta
      self.active_channels[(src,trg)][1] = alpha
      self.active_channels[(src,trg)][2] = beta
      


  def run_single_transaction(self,
                             transaction_id,
                             amount,
                             src,trg,
                             graph):
    
    result_bit = 0
    try:
      path = nx.shortest_path(graph, source=src, target=trg, weight="weight", method='dijkstra')
      
    except nx.NetworkXNoPath:
      return None,-1
    val = self.get_path_value(path,graph)
    result_bit = 1
    return path,result_bit  




 
  def run_simulation(self, count, amount, action):
      #print("simulating random transactions...")
      

      #Graph Pre-Processing
      if self.graph.has_edge(self.src, self.trg):
        self.graph[self.src][self.trg]['weight'] = action[0]*amount + action[1]



      #Run Transactions
      if self.fixed_transactions : 
        transactions = self.transactions
      else :
        transactions = generating_transactions.generate_transactions(amount, count, self.node_variables, self.epsilon, self.active_providers)
      transactions = transactions.assign(path=None)
      transactions['path'] = transactions['path'].astype('object')
   
      for index, transaction in transactions.iterrows(): 
        src,trg = transaction["src"],transaction["trg"]
        if (not src in self.graph.nodes()) or (not trg in self.graph.nodes()):
          path,result_bit = [] , -1
        else : 
          path,result_bit = self.run_single_transaction(transaction["transaction_id"],amount,transaction["src"],transaction["trg"],self.graph) 
          
        if result_bit == 1 : #successful transaction
            self.update_network_data(path,amount)
            transactions.at[index,"result_bit"] = 1
            transactions.at[index,"path"] = path

        elif result_bit == -1 : #failed transaction
            transactions.at[index,"result_bit"] = -1   
            transactions.at[index,"path"] = []
      # print("random transactions ended succussfully!")
      return transactions    #contains final result bits  #contains paths


 
 
  """
  getting the statistics
  """

  def get_balance(self,src,trg,channel_id):
      self.sync_network_dictionary()
      return self.network_dictionary[(src,trg)][0]


  def get_capacity(self,src,trg,channel_id):
      self.sync_network_dictionary()
      return self.network_dictionary[(src,trg)][3]



  def get_network_dictionary(self):
    return self.network_dictionary


  def get_k(self,src,trg,channel_id, transactions):
    num = 0
    for index, row in transactions.iterrows():
        path = row["path"]  
        for i in range(len(path)-1) :
          if (path[i]==src) & (path[i+1]==trg) :
              num += 1
    return num



  def get_total_fee(self,path) :
    self.sync_network_dictionary()
    alpha_bar = 0
    beta_bar = 0
    for i in range(len(path)-1):
      src = path[i]
      trg = path[i+1]
      src_trg = self.network_dictionary[(src,trg)]
      alpha_bar += src_trg[1]
      beta_bar += src_trg[2]
    return alpha_bar,beta_bar



  def find_rebalancing_cycle(self,rebalancing_type, src, trg, channel_id, rebalancing_amount):
      rebalancing_graph = self.generate_graph(rebalancing_amount)  
      cheapest_rebalancing_path = []
      
      alpha_bar = 0
      beta_bar = 0
      reult_bit = -1

      if rebalancing_type == -1 : #clockwise
          if (not src in rebalancing_graph.nodes()) or (not trg in rebalancing_graph.nodes()) or (not rebalancing_graph.has_edge(trg, src)):
            return -4,None,0,0
          if  rebalancing_graph.has_edge(src,trg):
            rebalancing_graph.remove_edge(src,trg)  
          cheapest_rebalancing_path,result_bit = self.run_single_transaction(-1,rebalancing_amount,src,trg,rebalancing_graph) 
          if result_bit == -1 :
            return -5,None,0,0
          elif result_bit == 1 :
            cheapest_rebalancing_path.append(src)
            alpha_bar,beta_bar = self.get_total_fee(cheapest_rebalancing_path)
            

      elif rebalancing_type == -2 : #counter-clockwise
          if (not trg in rebalancing_graph.nodes()) or (not src in rebalancing_graph.nodes()) or (not rebalancing_graph.has_edge(src, trg)):
            return -6,None,0,0
          if  rebalancing_graph.has_edge(trg,src):
            rebalancing_graph.remove_edge(trg,src)  
          cheapest_rebalancing_path,result_bit = self.run_single_transaction(-2,rebalancing_amount,trg,src,rebalancing_graph) 
          if result_bit == -1 :
            return -7,None,0,0
          elif result_bit == 1 :
            cheapest_rebalancing_path.insert(0,src)
            alpha_bar,beta_bar = self.get_total_fee(cheapest_rebalancing_path)
            
   
      
      return result_bit,cheapest_rebalancing_path,alpha_bar,beta_bar
      



      

  def get_coeffiecients(self,action,transactions,src,trg,channel_id, simulation_amount, onchain_transaction_fee):
        k = self.get_k(src,trg,channel_id,transactions)
        tx = simulation_amount*k
        rebalancing_fee, rebalancing_type  = self.operate_rebalancing(action[2],src,trg,channel_id,onchain_transaction_fee)
        return k,tx, rebalancing_fee, rebalancing_type



  def operate_rebalancing(self,gamma,src,trg,channel_id,onchain_transaction_fee):
    fee = 0
    if gamma == 0 :
      return 0,0  # no rebalancing
    elif gamma > 0 :
      rebalancing_type = -1 #clockwise
      result_bit, cheapest_rebalancing_path, alpha_bar, beta_bar = self.find_rebalancing_cycle(rebalancing_type, src, trg, channel_id, gamma)
      if result_bit == 1 :
        cost = alpha_bar*gamma + beta_bar
        if cost <= onchain_transaction_fee:
          self.update_network_data(cheapest_rebalancing_path, gamma)
          fee = cost
        else :
          self.onchain_rebalancing(gamma,src,trg,channel_id)
          fee = onchain_transaction_fee
          rebalancing_type = -3 #onchain
      
      else :
        self.onchain_rebalancing(gamma,src,trg,channel_id)
        fee = onchain_transaction_fee
        rebalancing_type = -3

      return fee, rebalancing_type

    else :
      rebalancing_type = -2 #counter-clockwise
      gamma = gamma*-1    
      result_bit,cheapest_rebalancing_path, alpha_bar, beta_bar = self.find_rebalancing_cycle(rebalancing_type, src, trg, channel_id, gamma)
      if result_bit == 1 :
        cost = alpha_bar*gamma + beta_bar
        if cost <= onchain_transaction_fee:
          self.update_network_data(cheapest_rebalancing_path, gamma)
          fee = cost
        else :
          self.onchain_rebalancing(gamma,trg,src,channel_id)
          fee = onchain_transaction_fee
          rebalancing_type = -3 #onchain
      
      elif result_bit == -1: 
        self.onchain_rebalancing(gamma,trg,src,channel_id)
        fee = onchain_transaction_fee
        rebalancing_type = -3

      return fee, rebalancing_type
