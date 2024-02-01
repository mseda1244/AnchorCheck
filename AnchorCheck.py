import pandas as pd
import re
import enmscripting
import socks
import socket
import numpy as np


# function to dyn port setup for enabling ENM tunnel:
def proxy(dyn_port):
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS4, "127.0.0.1", dyn_port)
    socket.socket = socks.socksocket


# function to process the command cmedit and get the output:
def df_builter(command):
    terminal1 = session.terminal()
    response1 = terminal1.execute(command)
    s = ""
    for line2 in response1.get_output():
        s+= str(line2+ '\n')
    print ("Response:")
    s = re.sub(r'^[^\n]*\n', '', s)
    s = re.sub(".*instance.*\n?","",s)
    # print(s)
    df = pd.read_csv(pd.compat.StringIO(s), sep='\t', lineterminator='\n')
    # df_mo = df
    # del [df]
    df_scope = df.loc[df['NodeId'].str.contains("Scope:")]
    df = df[df['NodeId'].str.contains("SubNetwork")==False]
    df = df[df['NodeId'].str.contains("NodeId")==False]
    df = df[df['NodeId'].str.contains("Scope")==False]
    df = df[df['NodeId'].str.contains("Error ")==False]
    return df_scope, df
    del [df_scope]
    del [df]


# Connecting to ENM:
enm_url_https = 'https://enmctpa.br.telefonica'
# enm_url_https='https://enmctpb.br.telefonica'
enm_proxy_port = 1080
enm_user = '80868205'
enm_pass = 'Msed@2023_4'
proxy(enm_proxy_port)
session = enmscripting.open(str(enm_url_https), str(enm_user), str(enm_pass))

# retrieving 5g site list:
df_5g_sites = pd.read_csv("Sites_5G.txt", header=None, names=["Site_5G"])
print ("Lista de sites 5G:{}".format(df_5g_sites['Site_5G'].tolist()))
site_lst=df_5g_sites['Site_5G'].values.tolist()
collection=";".join(site_lst)

# dataframe gnbid:
command = 'cmedit get ' + collection + ' GNBCUCPFunction.gNBId -t'
df_nr_gnbid_erro, df_nr_gnbid = df_builter(command)
df_nr_gnbid.to_csv('df_nr_gnbid.csv', index=False, header=True)
print ("Lista de sites|gNBid analisados:" + " [" + str(len(df_nr_gnbid)) + " sites]")
print (df_nr_gnbid)

# dataframe ssbfreq:
command = 'cmedit get ' + collection + ' NRCellDU.ssbfrequency -t'
df_ssbfreq_init_erro, df_ssbfreq_init = df_builter(command)
df_ssbfreq_group = df_ssbfreq_init.groupby(['NodeId', 'ssbFrequency']).size().to_frame().reset_index()
lst_ssbfreq = []
for i in site_lst:
    df_tmp = df_ssbfreq_group.loc[df_ssbfreq_group.NodeId==i]
    lst_ssbfreq.append(df_tmp['ssbFrequency'].tolist())
data = {'NodeId': site_lst,
        'ssbFrequency': lst_ssbfreq}
df_ssbfreq = pd.DataFrame(data)
print (df_ssbfreq)
df_ssbfreq.to_csv('df_ssbfreq.csv', index=False, header=True)

# dataframe gnb => TermPointToENodeB:
command = 'cmedit get ' + collection + ' TermPointToENodeB.operationalState -t'
df_nr_tp_erro, df_nr_tp = df_builter(command)
nr_tp_group = df_nr_tp.groupby(['NodeId', 'operationalState']).size()
df_nr_tp_group = nr_tp_group.to_frame()
df_nr_tp_group.rename(columns={0: "#"}, inplace=True)
df_nr_tp_group.to_csv('df_nr_tp_group.csv', index=True, header=True)
df_nr_tp = pd.merge(df_nr_tp, df_nr_gnbid, on='NodeId', how='left')
df_nr_tp.to_csv('df_nr_tp.csv', index=True, header=False)
print ("TermpointtoEnodeB status:")
print(df_nr_tp)
print ("TermpointtoEnodeB status Count:")
print(df_nr_tp_group)

# dataframe NodeId x eNBId rede:
command = 'cmedit get * ENodeBFunction.enbid -t'
df_4g_enbid_erro, df_4g_enbid = df_builter(command)
df_4g_enbid['eNBId'] = df_4g_enbid['eNBId'].fillna(0)
lst_enbid_str = df_4g_enbid['eNBId'].tolist()
lst_enbid_str = [str(int(i)) for i in lst_enbid_str]
print (lst_enbid_str)
df_4g_enbid['eNBId'] = lst_enbid_str
df_4g_enbid.to_csv('df_4g_enbid.csv', index=False, header=True)

# dataframe gNodeb x eNodeId ancoras x eNBId ancoras:
lst_anchor = []
lst_enbid = []
lst_tmp = df_nr_tp['ExternalENodeBFunctionId'].values.tolist()
for id in lst_tmp:
    enbid = re.split('_|-', id)[-1]
    lst_enbid.append(enbid)
    if str(enbid) in df_4g_enbid['eNBId'].values:
        lst_anchor.append(df_4g_enbid.loc[df_4g_enbid['eNBId']==str(enbid)]['NodeId'].values[0])
    else:
        lst_anchor.append("Not_Found")
df_nr_tp['Anchor'] = lst_anchor
df_nr_tp['eNBId_Anchor'] = lst_enbid
df_nr_tp_disabled = df_nr_tp[df_nr_tp.operationalState=="DISABLED"]
df_nr_tp.to_csv('df_nr_tp.csv', index=False, header=True)
df_nr_tp_disabled.to_csv('df_nr_tp_disabled.csv', index=False, header=True)
print('The following GNB TermPointToENodeBId has operationalState=DISABLED: [' + str(len(df_nr_tp_disabled)) + ' of ' +  str(len(df_nr_tp)) + ' occurrences]')
print(df_nr_tp_disabled)
print('Complete list of TermPointToENodeBId: [' + str(len(df_nr_tp)) + ' occurrences]')
print(df_nr_tp)

# Check of features
collection_anchor=";".join(lst_anchor)
print (collection_anchor)
command = 'cmedit get ' + collection_anchor + ' FeatureState.(FeatureStateId=="CXC4012371",serviceState,description);FeatureState.(FeatureStateId=="CXC4012381",serviceState,description); FeatureState.(FeatureStateId=="CXC4012504",serviceState,description);FeatureState.(FeatureStateId=="CXC4011559",serviceState,description);FeatureState.(FeatureStateId=="CXC4012324",serviceState,description);FeatureState.(FeatureStateId=="CXC4012218",serviceState,description);FeatureState.(FeatureStateId=="CXC4012324",serviceState,description) -t'
df_4g_cxc_erro, df_4g_cxc = df_builter(command)
df_4g_cxc_off = df_4g_cxc[df_4g_cxc.serviceState=="INOPERABLE"]
# Tirar linhas repetidas ja que um LTE pode ser ancora de mais de um 5G:
df_4g_cxc_off = df_4g_cxc_off.drop_duplicates()
df_4g_cxc_off.to_csv('df_4g_cxc_off.csv', index=False, header=True)
print("The anchors have " + str(len(df_4g_cxc_off)) + " of essential features INOPERABLE:" )
print (df_4g_cxc_off)

# Check primaryUpperLayerInd
command = 'cmedit get ' + collection_anchor + (' EUtranCellFDD.(endcAllowedPlmnList,'
                                               'primaryUpperLayerInd,mappingInfo.(mappingInfoSIB24)) -t')
df_4g_endcplmn_erro, df_4g_endcplmn = df_builter(command)
df_4g_endcplmn.to_csv('df_4g_endcplmn.csv', index=False, header=True)
df_4g_endcplmn['endcAllowedPlmnList'] = df_4g_endcplmn['endcAllowedPlmnList'].fillna("not found")
df_4g_endcplmn_inc = df_4g_endcplmn[(df_4g_endcplmn['endcAllowedPlmnList'].str.contains('.mcc=724.*', regex=True)==False)|
                 (df_4g_endcplmn['primaryUpperLayerInd'].str.contains('OFF', regex=True)==True)|
                 (df_4g_endcplmn['mappingInfo'].str.contains('.NOT_MAPPED', regex=True)==True)]
df_4g_endcplmn_inc.to_csv('df_4g_endcplmn_inc.csv', index=False, header=True)
print('Anchors with incorrect endcAllowedPlmnList, mappingInfo, '
      'primaryUpperLayerInd and sib1AltSchInfo parameters settings: [' + str(len(df_4g_endcplmn_inc)) + ' occurrences]')
print(df_4g_endcplmn_inc)

# Check EndcProfileId==1,meNbS1TermReqArpLev==0,splitNotAllowedUeArpLev==0
command = 'cmedit get ' + collection_anchor + (' EndcProfile.(EndcProfileId==1,'
                                               'meNbS1TermReqArpLev==0,splitNotAllowedUeArpLev==0) -t')
df_4g_endcprof1_erro, df_4g_endcprof1 = df_builter(command)
df_4g_endcprof1.to_csv('df_4g_endcprof1.csv', index=False, header=True)
df_4g_endcprof1_inc = df_4g_endcprof1[(df_4g_endcprof1.meNbS1TermReqArpLev!=0)|
                                  (df_4g_endcprof1.splitNotAllowedUeArpLev!=0)]
if df_4g_endcprof1_inc.empty == True:
    print('Is the DataFrame empty: ' + str(df_4g_endcprof1_inc.empty))
    d = pd.DataFrame('0', index=np.arange(1), columns=df_4g_endcprof1_inc.columns)
    df_4g_endcprof1_inc = pd.concat([df_4g_endcprof1_inc,d])
df_4g_endcprof1_inc.to_csv('df_4g_endcprof1_inc.csv', index=False, header=True)
print ('Check EndcProfileId=1 meNbS1TermReqArpLev!=0,'
       'splitNotAllowedUeArpLev!=0: [' + str(len(df_4g_endcprof1_inc)) + ' occurrences]')
print(df_4g_endcprof1_inc)

# Check EndcProfileId==2,meNbS1TermReqArpLev==15,splitNotAllowedUeArpLev==0
command = 'cmedit get ' + collection_anchor + ' EndcProfile.(EndcProfileId==2,meNbS1TermReqArpLev==15,splitNotAllowedUeArpLev==0) -t'
df_4g_endcprof2_erro, df_4g_endcprof2 = df_builter(command)
print(df_4g_endcprof2)
df_4g_endcprof2.to_csv('df_4g_endcprof2.csv', index=False, header=True)
df_4g_endcprof2_inc = df_4g_endcprof2[(df_4g_endcprof2.meNbS1TermReqArpLev!=15)|
                                  (df_4g_endcprof2.splitNotAllowedUeArpLev!=0)]
df_4g_endcprof2['meNbS1TermReqArpLev'] = df_4g_endcprof2['meNbS1TermReqArpLev'].astype('int')
if df_4g_endcprof2_inc.empty == True:
    print('Is the DataFrame empty: ' + str(df_4g_endcprof2_inc.empty))
    d = pd.DataFrame('0', index=np.arange(1), columns=df_4g_endcprof2_inc.columns)
    df_4g_endcprof2_inc = pd.concat([df_4g_endcprof2_inc,d])
df_4g_endcprof2_inc.to_csv('df_4g_endcprof2_inc.csv', index=False, header=True)
print ('Check EndcProfileId=2 meNbS1TermReqArpLev!=15,'
       'splitNotAllowedUeArpLev!=0: [' + str(len(df_4g_endcprof2_inc)) + ' occurrences]' )
print(df_4g_endcprof2_inc)

# Check endcProfileRef != EndcProfile=1 for qci9
command = 'cmedit get ' + collection_anchor + (' QciProfilePredefined.(QciProfilePredefinedId=="qci3",endcProfileRef);'
                                               'QciProfilePredefined.(QciProfilePredefinedId=="qci9",endcProfileRef) -t')
df_4g_qciprof_erro, df_4g_qciprof = df_builter(command)
lst_prof = []
for prof in df_4g_qciprof['endcProfileRef']:
    endcProfileRef = prof.split(",")[-1]
    lst_prof.append(endcProfileRef)
df_4g_qciprof ['endcProfileRef_short'] = lst_prof
df_4g_qciprof.to_csv('df_4g_qciprof.csv', index=False, header=True)
df_4g_qciprof_inc = df_4g_qciprof[(df_4g_qciprof.endcProfileRef_short!='EndcProfile=1')]
df_4g_qciprof_inc.to_csv('df_4g_qciprof_inc.csv', index=False, header=True)
print ('Check endcProfileRef != EndcProfile=1 for qci9: [' + str(len(df_4g_qciprof_inc)) + ' occurrences]' )
print(df_4g_qciprof_inc)

# Check endcProfileRef != EndcProfile=2 for qci5
command = 'cmedit get ' + collection_anchor + ' QciProfilePredefined.(QciProfilePredefinedId=="qci5",endcProfileRef) -t'
df_4g_qciprof5_erro, df_4g_qciprof5 = df_builter(command)
lst_prof = []
for prof in df_4g_qciprof5['endcProfileRef']:
    endcProfileRef = prof.split(",")[-1]
    lst_prof.append(endcProfileRef)
df_4g_qciprof5 ['endcProfileRef_short'] = lst_prof
df_4g_qciprof5.to_csv('df_4g_qciprof5.csv', index=False, header=True)
df_4g_qciprof5_inc = df_4g_qciprof5[(df_4g_qciprof5.endcProfileRef_short!='EndcProfile=2')]
df_4g_qciprof5_inc.to_csv('df_4g_qciprof5_inc.csv', index=False, header=True)
print ('Check endcProfileRef != EndcProfile=2 for qci5: [' + str(len(df_4g_qciprof5_inc)) + ' occurrences]' )
print(df_4g_qciprof5_inc)

# Retrieve GUtranFreqRelationId and EUtranCellFDDId
command = 'cmedit get ' + collection_anchor + ' GUtranFreqRelation.(gUtranFreqRelationId,endcB1MeasPriority,gUtranSyncSignalFrequencyRef) -t'
df_4g_gutranfreqrel_erro, df_4g_gutranfreqrel = df_builter(command)
command = 'cmedit get ' + collection_anchor + ' EUtranCellFDD.(EUtranCellFDDId) -t'
df_4g_eutrancellfdd_erro, df_4g_eutrancellfdd = df_builter(command)
df_4g_gutranfreqrel.to_csv('df_4g_gutranfreqrel.csv', index=True, header=True)
df_4g_eutrancellfdd.to_csv('df_4g_eutrancellfdd.csv', index=True, header=True)
df_groug=df_4g_gutranfreqrel.groupby(['NodeId','gUtranFreqRelationId']).size()
df_4g_gutranfreqrel_group = df_groug.to_frame()
df_4g_gutranfreqrel_group.to_csv('df_4g_gutranfreqrel_group.csv', index=True, header=True)
print(df_4g_gutranfreqrel_group)
df_groug2=df_4g_eutrancellfdd.groupby(['NodeId']).size()
df_4g_eutrancellfdd_group = df_groug2.to_frame()
df_4g_eutrancellfdd_group.rename(columns={0:"Células_4G"},inplace=True)
df_4g_gutranfreqrel_group2 = pd.merge(df_4g_eutrancellfdd_group,df_4g_gutranfreqrel_group,on='NodeId',how='left')
df_4g_gutranfreqrel_group2.rename(columns={0:"GUtranfreqRel"},inplace=True)
print (df_4g_gutranfreqrel_group2)
df_4g_gutranfreqrel_group2.to_csv('df_4g_gutranfreqrel_group2.csv', index=True, header=True)

# Retrieve nr celllocalid and ssbfrequency
command = 'cmedit get ' + collection + ' nrcelldu.(celllocalid,ssbfrequency) -t'
df_nr_nrcelldu_erro, df_nr_nrcelldu = df_builter(command)
df_nr_gnbid_cellid_freq = pd.merge(df_nr_gnbid, df_nr_nrcelldu, on="NodeId")
print (df_nr_gnbid_cellid_freq)

# Retrieving lte => nr neighborcellref (plmn-gnbid-cellid)
command = 'cmedit get ' + collection_anchor + ' GUtranCellrelation.(neighborCellRef) -t'
df_4g_gutrancellrelation_erro, df_4g_gutrancellrelation = df_builter(command)
df_4g_gutrancellrelation
#print (df_4g_gutrancellrelation)
lst_neighborCellid = []
lst_neighborGnbid = []
lst_neighborplmn =[]
for n in df_4g_gutrancellrelation['neighborCellRef']:
    neighborCell = n.split(",")[-1]
    neighborCellid = neighborCell.split("-")[-1]
    neighborGnbid = int(neighborCell.split("-")[-2])
    neighborplmn = neighborCell.split("-")[-3].split("=")[-1]
    lst_neighborCellid.append(neighborCellid)
    lst_neighborGnbid.append(neighborGnbid)
    lst_neighborplmn.append(neighborplmn)
df_4g_gutrancellrelation ['neighborGnbid'] = (lst_neighborGnbid)
df_4g_gutrancellrelation ['neighborCellid'] = (lst_neighborCellid)
df_4g_gutrancellrelation ['neighborplmn'] = (lst_neighborplmn)
# filtrar gnbid,ancora contido em cada linha de df_nr_tp em df_4g_gutrancellrelation
df_4g_gutrancellrelation ['gnbid_anchor'] = df_4g_gutrancellrelation ['neighborGnbid'].apply(str) + df_4g_gutrancellrelation['NodeId']
df_4g_gutrancellrelation ['gutran_neighbor'] = df_4g_gutrancellrelation['GUtranFreqRelationId'] + "|" +df_4g_gutrancellrelation ['neighborplmn'] + "-" + df_4g_gutrancellrelation ['neighborGnbid'].apply(str) + "-" + df_4g_gutrancellrelation ['neighborCellid']
df_4g_gutrancellrelation.to_csv('df_4g_gutrancellrelation.csv', index=True, header=True)
print (df_4g_gutrancellrelation)

# Filling df_nr_tp
def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

lst_anchoring_check = []
lst_guntrancellrelation = []
lst_guntrancellrelation_tmp = []
df_neigh_group = df_4g_gutrancellrelation.groupby(['GUtranFreqRelationId']).size().reset_index(name='freq')[
    'GUtranFreqRelationId'].to_frame()
lst_freq = (df_neigh_group['GUtranFreqRelationId'].tolist())
print (lst_freq)
dict_freq = {}
for i in lst_freq: dict_freq.update({i: []})
p = 0
for index, row in df_nr_tp.iterrows():
    # print(row['gNBId'], row['Anchor'])
    gNBId = row['gNBId']
    Anchor = row['Anchor']
    gnbid_anchor = str(gNBId) + Anchor
    p = p + 1
    # print (p)
    if len(df_4g_gutrancellrelation.loc[df_4g_gutrancellrelation.gnbid_anchor == gnbid_anchor, 'gnbid_anchor']) > 0:
        lst_anchoring_check.append("Yes")
        df_tmp = df_4g_gutrancellrelation.loc[df_4g_gutrancellrelation.gnbid_anchor == gnbid_anchor]
        df_neigh_group = df_tmp.groupby(['gutran_neighbor']).size().reset_index(name='count')
        df_freq_group = df_tmp.groupby(['GUtranFreqRelationId']).size().reset_index(name='count')
        lst_1 = df_neigh_group['gutran_neighbor'].tolist()
        lst_2 = df_neigh_group['count'].apply(str).tolist()
        lst_3 = df_freq_group['GUtranFreqRelationId'].tolist()
        lst_4 = df_freq_group['count'].apply(str).tolist()
        n = 0
        k = 0
        lst_diff = Diff(lst_freq, lst_3)
        if len(lst_diff) != 0:
            for i in lst_diff: dict_freq[i].append('0')
        for i in lst_1:
            rel = lst_1[n] + "|[" + lst_2[n] + "]"
            lst_guntrancellrelation_tmp.append(rel)
            n = n + 1
        lst_guntrancellrelation.append(lst_guntrancellrelation_tmp)
        lst_guntrancellrelation_tmp = []
        for i in lst_3:
            # print (i)
            dict_freq[i].append(lst_4[k])
            k = k + 1
    else:
        lst_anchoring_check.append("No")
        # print ("No")
        lst_guntrancellrelation.append("No Gutrancell Defined for this GNBid")
        for i in dict_freq:
            dict_freq[i].append('0')

df_nr_tp['guntrancellrelation'] = lst_guntrancellrelation
df_nr_tp['anchoring_check'] = lst_anchoring_check
for i in dict_freq:
    df_nr_tp[i] = dict_freq[i]

df_nr_tp = pd.merge(df_nr_tp, df_ssbfreq, on='NodeId', how='left')
print (df_nr_tp)
df_nr_tp.to_csv('df_nr_tp.csv', index=True, header=True)