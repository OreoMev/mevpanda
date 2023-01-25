from flask import Flask
import requests
import json
import os
from threading import Thread
from web3 import Web3
from hexbytes import HexBytes
import pandas as pd
import numpy as np
import glob
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time
web3Provider="Add your own web3 provider here" 
etherscanAPIkey="ADD_YOUR_API_KEY" #Add your etherscan api key, can be a free account but watch the rate limit
beaconApiURL="ADD_YOUR_BEACON_URL" # Add an url of a beacon client with API

web3 = Web3(Web3.HTTPProvider(web3Provider))

app = Flask(__name__)
#Define starting slot and block (should be the same slot->block)
startSlot=5535772
latestSlotStored=0
startBlock=16368080
latestBlockStored=0

relays=[
{"id":"eden","url":"https://relay.edennetwork.io/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"relayoor","url":"https://relayooor.wtf/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"manifold","url":"https://mainnet-relay.securerpc.com/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"flashbots","url":"https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"blocknative","url":"https://builder-relay-mainnet.blocknative.com/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"bloxrouteMaxProfit","url":"https://bloxroute.max-profit.blxrbdn.com/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"bloxrouteEthical","url":"https://bloxroute.ethical.blxrbdn.com/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"bloxrouteRegulated","url":"https://bloxroute.regulated.blxrbdn.com/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"agnostic","url":"https://agnostic-relay.net/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"},
{"id":"ultrasound","url":"https://relay.ultrasound.money/relay/v1/data/bidtraces/proposer_payload_delivered?limit=100"}
]
#Locking functions (prevent double loading of data)
def lock(name):
    with open(name+"Lock","w") as f:
        f.write("IN PROGRESS")
def unlock(name):
    if os.path.exists(name+"Lock"):
        os.remove(name+"Lock")
def checkLock(name):
    if os.path.exists(name+"Lock"):
        return True
    else:
        return False
def clearLocks():
    unlock("block")
    unlock("relay")
    unlock("verify")
    unlock("slot")
#data gathering functions
def getAndStoreBlock(n):
    #
    # Need to figure out how to json.dumps the object with all the transactions as well
    ## full_transactions = False
    #
    #
    n_ = n
    try:
        print (n_)
        block= web3.eth.get_block(n_, full_transactions = True)
        blockN=block["number"]
        json_object = Web3.toJSON(block)
       
        if (n_=="latest"):
            time.sleep(10)
        url= "https://api.etherscan.io/api?module=block&action=getblockreward&blockno={}&apikey=".format(blockN)+etherscanAPIkey
        block1 = requests.get(url)
        block1 = json.loads(block1.text)
        blockReward=int(block1['result']['blockReward'])
        json_object=json.loads(json_object)
        json_object["blockReward"]=blockReward
        json_object = json.dumps(json_object, indent=4)
        with open("blocks/"+str(blockN)+".json", "w") as outfile:
            outfile.write(json_object)
        return int(blockN)
    except Exception as e:
        print (e)

        return ("ERR")
def getMissingBlocks():
    global startBlock
    global latestBlockStored
    for i in range (startBlock,latestBlockStored+1):
        if os.path.exists("blocks/"+str(i)+".json"):
            continue
        else:
            getAndStoreBlock(i)
def getAndStoreSlot(n):
    n_ = n
    url= beaconApiURL+"/eth/v2/beacon/blocks/{}".format(n_)
    try:
        slot = requests.get(url)
        slot = json.loads(slot.text)
        try:
            if (slot["code"]==404):
                with open("slots/"+str(n_)+".json", "w") as outfile:
                    outfile.write("""{"status": "skippedSlot"}""")
                    return int(n_)
        except:
            err="None"
        slotN=slot["data"]["message"]["slot"]
        json_object = json.dumps(slot, indent=4)
        with open("slots/"+str(slotN)+".json", "w") as outfile:
            outfile.write(json_object)
        return int(slotN)
    except Exception as e:
        print (n_)
        print(e)
        print (slot)
        return ("ERR")
def getMissingSlots():
    global startSlot
    global latestSlotStored
    for i in range (startSlot,latestSlotStored+1):
        if os.path.exists("slots/"+str(i)+".json"):
            continue
        else:
            getAndStoreSlot(i)
def getRelayData(id,url,cursor):
    global startSlot
    if (cursor=="latest"):
        url=url
    else:
        url=url+"&cursor="+str(cursor)
    try:
      x = requests.get(url)
      y = json.loads(x.text)
      for slot in y:
          json_object = json.dumps(slot)
          if (os.path.exists("relayData/"+str(slot["slot"])+"_"+id+".json")):
              return "0"
          elif (int(slot["slot"])<startSlot):
              return "0"
          else:
            with open("relayData/"+str(slot["slot"])+"_"+id+".json", "w") as outfile:
                outfile.write(json_object)
            returnCursor=slot["slot"]
      return (int(returnCursor)-1)
    except Exception as e:
      print(e)
      return "ERR"
def verify(filename):
    #First it checks if this slot was already verified
    print (filename)
    if os.path.exists("mevboost/"+str(filename)+"_verified.json"):
        return "0"
    #Then it stores all the data from the relay data file in variables for later
    ff=filename
    relayId=ff.split("_")[1].split(".json")[0]
    f=open("relayData/"+filename)
    relayData=json.load(f)
    f.close()
    slot=int(relayData["slot"])
    builder=relayData["builder_pubkey"]
    proposer=relayData["proposer_pubkey"]
    proposer_fee_recipient=relayData["proposer_fee_recipient"].lower()
    promisedETH=int(relayData["value"])
    #Check if slot file exists and get blockNumber from it
    if os.path.exists("slots/"+str(slot)+".json"):
        f=open("slots/"+str(slot)+".json")
        slotData=json.load(f)
        f.close()
        try:
            blockNumber=slotData["data"]["message"]["body"]["execution_payload"]["block_number"]
            
        except:
            blockNumber=0
        try:
            extraData=bytes.fromhex(slotData["data"]["message"]["body"]["execution_payload"]["extra_data"][2:]).decode("utf-8")
        except:
            extraData=""

    else:
        return "MISSING_SLOT:"+str(slot)
    if (blockNumber==0):
        verifiedData={
        "relayId":relayId,
        "slot":slot,
        "block_number":0,
        "builder":builder,
        "proposer":proposer,
        "proposer_fee_recipient":proposer_fee_recipient,
        "promised_ETH":promisedETH,
        "delivered_ETH":0,
        "builderProfit":0,
        "blockReward":0,
        "builderBalanceAtBlock":0,
        "builderBalanceBeforeBlock":0,
        "extraData":""
    }
        json_object = json.dumps(verifiedData, indent=4)
        with open("mevboost/"+str(filename)+"_verified.json","w") as outfile:
            outfile.write(json_object)
        
        return ("1")
    #If block number is not 0 (the slot was not skipped), it opens the block file
    if os.path.exists("blocks/"+str(blockNumber)+".json"):
        f=open("blocks/"+str(blockNumber)+".json")
        blockData=json.load(f)
        f.close()
    else:
        return "MISING_BLOCK:"+str(blockNumber)
    blockReward=int(blockData["blockReward"])
    miner=blockData["miner"].lower()
    #Setting delivered eth to 0
    deliveredETH=0
    deliveryTransaction=False
    #Now to handling the different cases on how the mevboost reward was delivered to proposer_fee_recipient
    #Option 1: proposer_fee_receipient was set as a miner
    if (miner==proposer_fee_recipient):
        deliveredETH=deliveredETH+blockReward
        url="https://api.etherscan.io/api?module=account&action=txlistinternal&address="+proposer_fee_recipient+"&startblock="+str(blockNumber)+"&endblock="+str(blockNumber)+"&apikey="+etherscanAPIkey
        internalData = requests.get(url)
        internalDataJson = json.loads(internalData.text)
        for t in internalDataJson["result"]:
            if (t["to"].lower()==proposer_fee_recipient):
                deliveredETH=deliveredETH+ int(t["value"])

    #Option 2: If any transaction was sent from the miner (block builder) to proposer_fee_recipient address (for loop throug all transactions)
    fromList=[]
    for transaction in blockData["transactions"]:
        try:
            if (transaction["to"].lower()==proposer_fee_recipient):
                deliveredETH=deliveredETH+int(transaction["value"])
                deliveryTransaction=True
                deliveryAddressFrom=transaction["from"].lower()
                fromList.append(deliveryAddressFrom)
        except:
            a=1 #probably contract creation
    profit=0
    balanceAtBlock=0
    balanceBeforeBlock=0
    if (deliveryTransaction and miner!=proposer_fee_recipient):
        totalDelivered=deliveredETH
        incomeBlockReward=blockReward
        #Need to get miner balance before and at the block to figure out the difference
        blockNumber=int(blockNumber)
        balanceAtBlock= web3.eth.getBalance(Web3.toChecksumAddress(miner),blockNumber)
        balanceBeforeBlock= web3.eth.getBalance(Web3.toChecksumAddress(miner),blockNumber-1)
        profit=balanceAtBlock-balanceBeforeBlock
    elif (len(list(dict.fromkeys(fromList)))==1):
        totalDelivered=deliveredETH
        incomeBlockReward=blockReward
        #Need to get miner balance before and at the block to figure out the difference
        blockNumber=int(blockNumber)
        balanceAtBlock= web3.eth.getBalance(Web3.toChecksumAddress(fromList[0]),blockNumber)
        balanceBeforeBlock= web3.eth.getBalance(Web3.toChecksumAddress(fromList[0]),blockNumber)
        profit=balanceAtBlock-balanceBeforeBlock


    verifiedData={
        "relayId":relayId,
        "slot":slot,
        "block_number":blockNumber,
        "builder":builder,
        "proposer":proposer,
        "proposer_fee_recipient":proposer_fee_recipient,
        "promised_ETH":promisedETH,
        "delivered_ETH":deliveredETH,
        "builderProfit":profit,
        "blockReward":blockReward,
        "builderBalanceAtBlock":balanceAtBlock,
        "builderBalanceBeforeBlock":balanceBeforeBlock,
        "extraData":extraData
    }
    json_object = json.dumps(verifiedData, indent=4)
    with open("mevboost/"+str(filename)+"_verified.json","w") as outfile:
        outfile.write(json_object)
    return "0"

def genStats():
    dir_list = os.listdir("blocks")
    latestBlockStored=int(max(dir_list).split(".json")[0])
    df = pd.read_csv ('history.csv')
    df = df.assign(Difference=lambda x: ( x['delivered_ETH'] -x['promised_ETH']))
    df["delivered_ETH"]=df["delivered_ETH"].div(pow(10,18))
    df["promised_ETH"]=df["promised_ETH"].div(pow(10,18))
    df["builderProfit"]=df["builderProfit"].div(pow(10,18))
    df = df.assign(Blocks=lambda x: ( 1 ))
    sortedDF= df.sort_values(by=['slot'], ascending=False)
    sortedDF['OverpromisedBlock'] = sortedDF['Difference'].apply(lambda x: 1 if x < 0.00 else 0)
    sortedDF['ProfitBlock'] = sortedDF['builderProfit'].apply(lambda x: 1 if x > 0 else 0)
    sortedDF['PayedBlock'] = sortedDF['builderProfit'].apply(lambda x: 1 if x < 0 else 0)
    dfTotal = sortedDF.groupby('relayId').agg({'promised_ETH': 'sum',"delivered_ETH": ["sum","count","mean","median","max"],"Difference":"sum","OverpromisedBlock":"sum"})
    last7df = sortedDF.loc[sortedDF['slot'] > sortedDF['slot'].max() - 50400] 
    df7d = last7df.groupby('relayId').agg({'promised_ETH': 'sum',"delivered_ETH": ["sum","count","mean","median","max"],"Difference":"sum","OverpromisedBlock":"sum"})
    last24df = sortedDF.loc[sortedDF['slot'] > sortedDF['slot'].max() - 7200] 
    df24 = last24df.groupby('relayId').agg({'promised_ETH': 'sum',"delivered_ETH": ["sum","count","mean","median","max"],"Difference":"sum","OverpromisedBlock":"sum"})
    
    
    
    
    builder24df=last24df.sort_values('promised_ETH').drop_duplicates(subset=['builder', 'block_number'], keep='last')
    builder7df=last7df.sort_values('promised_ETH').drop_duplicates(subset=['builder', 'block_number'], keep='last')
    builderStats24=builder24df.groupby("builder").agg({'builderProfit': 'sum',"Blocks":sum,"relayId":"unique","ProfitBlock":sum,'PayedBlock':sum,"promised_ETH":["sum","mean"],"extraData":"unique"})
    builderStats7=builder7df.groupby("builder").agg({'builderProfit': 'sum',"Blocks":sum,"relayId":"unique","ProfitBlock":sum,'PayedBlock':sum,"promised_ETH":["sum","mean"],"extraData":"unique"})





    top2024=last24df.sort_values(by=['promised_ETH'], ascending=False).head(20)
    top2024.to_json("api/top20_24h.json",orient="index")
    top207=last7df.sort_values(by=['promised_ETH'], ascending=False).head(20)
    top207.to_json("api/top20_7d.json",orient="index")

    builderToRelay24=last24df.groupby(["builder","relayId"]).agg({'promised_ETH': 'sum',"Blocks":"sum"})
    builderToRelay7=last7df.groupby(["builder","relayId"]).agg({'promised_ETH': 'sum',"Blocks":"sum"})

    builderStats24.to_json("api/builder_24h.json",orient="index")
    builderStats7.to_json("api/builder_7d.json",orient="index")
    builderToRelay24.to_json("api/builderToRelay_24h.json",orient="index")
    builderToRelay7.to_json("api/builderToRelay_7d.json",orient="index")
    df24.to_json("api/stats_24h.json",orient="index")
    df7d.to_json("api/stats_7d.json",orient="index")
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    builderStats24.columns = ['_'.join(col).strip() for col in builderStats24.columns.values]
    final_df = builderStats24.sort_values(by=['Blocks_sum'], ascending=False)
    t = int(time.time())
    updateObject = {
        "latestBlock":latestBlockStored,
        "updateTime" : t
    }
    json_object = json.dumps(updateObject)
    with open('update.json', 'w') as fo:
        fo.write(json_object)





def GenerateCSV():
    path_to_json = 'mevboost/'
    json_pattern = os.path.join(path_to_json,'*.json')
    file_list = glob.glob(json_pattern)
    dfs = []
    first=True
    df = pd.DataFrame()
    for file in file_list:
        f=open(file)
        data = json.load(f)
        pdData=pd.DataFrame([data])
        df=pd.concat([df, pdData])
    df.to_csv("history.csv")
def initScript():
    global startSlot
    global latestSlotStored
    global startBlock
    global latestBlockStored
    clearLocks()
    getAndStoreSlot(startSlot)
    getAndStoreSlot("head")
    dir_list = os.listdir("slots")
    latestSlotStored=int(max(dir_list).split(".json")[0])
    if (latestSlotStored-startSlot>len(dir_list)):
        print ("There are some slots missing, getting missing slots.")
        getMissingSlots()
    print ("Latest slot: "+str(latestSlotStored))
    getAndStoreBlock(startBlock)
    getAndStoreBlock("latest")
    dir_list = os.listdir("blocks")
    latestBlockStored=int(max(dir_list).split(".json")[0])
    if (latestBlockStored-startBlock>len(dir_list)):
        print ("There are some blocks missing, getting missing block")
        getMissingBlocks()
    print ("Latest block: "+str(latestBlockStored))

initScript()

@app.route("/updateSlots")
def updateSlots():
    if checkLock("slot"):
        return "UPDATE IN PROGRESS"
    else:
        lock("slot")
    Thread(target = SlotUpdater).start()
    return "OK"
def SlotUpdater():
    global latestSlotStored
    l=getAndStoreSlot("head")
    if (l!="ERR"):
        latestSlotStored=l
    getMissingSlots()
    unlock("slot")
    
@app.route("/updateBlocks")
def updateBlocks():
    if checkLock("block"):
        return "UPDATE IN PROGRESS"
    else:
        lock("block")
    Thread(target = BlockUpdater).start()
    return "OK"
def BlockUpdater():
    global latestBlockStored
    l=getAndStoreBlock("latest")
    if (l!="ERR"):
        latestBlockStored=l
    getMissingBlocks()
    unlock("block")


@app.route("/updateRelayData")
def updateRelayData():
    if checkLock("relay"):
        return "UPDATE IN PROGRESS"
    else:
        lock("relay")
    Thread(target = relayUpdater).start()
    return "OK"
def relayUpdater():
    for relay in relays:
        id= relay["id"]
        url= relay["url"]
        cursor= "latest"
        while (cursor!="0"):
            cursor=getRelayData(id,url,cursor)
            time.sleep(1)
            print (cursor)
            if (cursor=="ERR"):
                print ("error on relay: "+str(id))
                break
    unlock("relay")
@app.route("/verifyData")
def verifyData():
    if checkLock("verify"):
        return "UPDATE IN PROGRESS"
    else:
        lock("verify")
    Thread(target = dataVerify).start()
    return "OK"
def dataVerify():
    dir_list = os.listdir("relayData")
    for f in dir_list:
        if (f.endswith(".json")):
            t=verify(f)
    GenerateCSV()
    genStats()
    unlock("verify")


def doUpdateThing():
    updateSlots()
    updateBlocks()
    updateRelayData()
    verifyData()

scheduler = BackgroundScheduler()
job = scheduler.add_job(doUpdateThing, 'interval', minutes=5)
scheduler.start()
