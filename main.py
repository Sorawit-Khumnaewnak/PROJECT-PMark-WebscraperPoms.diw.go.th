import time
import requests
from pathlib import Path
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API (Method-POST) สำหรับดึง ID ของโรงงานทั้งหมดใน poms.diw.go.th
APT_GetIDAllFactory = "https://poms.diw.go.th/factory-ws/get/factory-list?"

# API (Method-POST) สำหรับดึงข้อมูลของแต่ละโรงงานแต่ละ ID ใน poms.diw.go.th
APT_GetResultFactory = "https://poms.diw.go.th/factory-ws/get/measurement-list/"


def GetDataCEMS():
    msg = {'measurement':1}
    GetMaxPage = (requests.post(APT_GetIDAllFactory, params=msg, verify=False).json())
    if GetMaxPage['message'] != "สำเร็จ":
        return []
    numMaxPage = GetMaxPage['data']['maxPage']
    FactoryID_ALL = []
    for i in range(numMaxPage):
        msg['page'] = str(i+1)
        GetData = (requests.post(APT_GetIDAllFactory, params=msg, verify=False).json())
        if (GetData['message'] == "สำเร็จ"):
            FactoryID_ALL.extend({"id": k['id'], "name": k['name']} for k in GetData['data']['items'])
    return FactoryID_ALL

def GetDataSTATION():
    msg = {'measurement':4}
    GetMaxPage = (requests.post(APT_GetIDAllFactory, params=msg, verify=False).json())
    if GetMaxPage['message'] != "สำเร็จ":
        return []
    numMaxPage = GetMaxPage['data']['maxPage']
    FactoryID_ALL = []
    for i in range(numMaxPage):
        msg['page'] = str(i+1)
        GetData = (requests.post(APT_GetIDAllFactory, params=msg, verify=False).json())
        if (GetData['message'] == "สำเร็จ"):
            FactoryID_ALL.extend({"id": k['id'], "name": k['name']} for k in GetData['data']['items'])
    return FactoryID_ALL

def GetAndRecord(listID, typeName, typeID, dataTime):
    # pathSave = f'record/{dataTime}/{typeName}'
    pathSave = f'record/{typeName}'
    # Create folder
    Path(pathSave).mkdir(parents=True, exist_ok=True)

    # Get data by id
    for i in listID:
        print(f"ID: {i['id']}, NAME: {i['name']} => Record")
        msg = {'type': str(typeID)}
        getDataById = (requests.post(APT_GetResultFactory+str(i['id']), params=msg, verify=False).json())
        if getDataById['message'] == "สำเร็จ":
            parameters = "".join(f"{getDataById['data']['parameters'][k]['name']}({getDataById['data']['parameters'][k]['unit']})," for k in getDataById['data']['parameters'])
            file = Path(f"{pathSave}/{i['name']}.csv")
            dataAdd = []
            for q in getDataById['data']['measurements']:
                measName = getDataById['data']['measurements'][q]['measName']
                try:
                    GetdateTime = str(getDataById['data']['measurements'][q]['recordedDate']).split(" ")
                    dataGet = GetdateTime[0]
                    timeGet = GetdateTime[1]
                except Exception:
                    dataGet = "-"
                    timeGet = "-"
                arrDate = [dataTime, measName, dataGet, timeGet]
                for r_id in getDataById['data']['measurements'][q]['parameters']:
                    if getDataById['data']['measurements'][q]['parameters'][r_id]['value'] is None:
                        if not str(getDataById['data']['measurements'][q]['parameters'][r_id]['errMsg']):
                            arrDate.append("-")
                        else:
                            arrDate.append(str(getDataById['data']['measurements'][q]['parameters'][r_id]['errMsg']))
                    else:
                        arrDate.append(str(getDataById['data']['measurements'][q]['parameters'][r_id]['value']))
                arrDate.append('\n')
                dataAdd.append(", ".join(arrDate))
            dataAdd = "".join(dataAdd)
            if file.is_file():
                # file exists
                with open(file, "a", encoding='utf-8-sig') as f:
                    f.write(dataAdd)
            else:
                with open(file, "w", encoding='utf-8-sig') as f:
                    f.write(f"Time,จุดตรวจววัด,วันที่,เวลา,{parameters}\n")
                    f.write(dataAdd)


eventLoopSec = 3600 # 1 Hr
sec = 0
while True:
    print(f"Sec: {sec}/{eventLoopSec}")
    if (sec == 0):
        # GET ID ALL ================================================================================
        dateTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        GetIDCEMS = GetDataCEMS()
        GetIDSTATION = GetDataSTATION()
        # ===========================================================================================
        # Get data by id and record in file =========================================================
        GetAndRecord(GetIDCEMS, 'CEMS', 1, dateTime)
        GetAndRecord(GetIDSTATION, 'STATION', 4, dateTime)
    if (sec == eventLoopSec):
        sec = 0
        continue
    sec += 1
    time.sleep(1)
