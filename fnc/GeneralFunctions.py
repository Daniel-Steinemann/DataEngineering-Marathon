import pandas as pd
from yattag import Doc, indent
import os, fnmatch
from datetime import datetime, timedelta, time, date


#### Clean blanks and spaces before headers ####
key = '' # key used to post
path = '' # project path to locate xml files
url = '' # destunation url to post 
cuid = '' # Client Unique ID
def clean_file(file):
    data = pd.read_excel(file, encoding="latin-1", dtype=object)
    first_col_is_correct = len([x for x in data.columns if 'Unnamed' in x]) < len(data.columns) // 2
    if not first_col_is_correct:
        i = 0
        for j, row in enumerate(data.head(10).fillna('').itertuples()):
            valid_row = len([x for x in row if x != '']) > len(row) // 2
            if valid_row:
                i = j
                break
        new_cols = [x for x in data.loc[i]]
        fixed_cols = []
        for _ in new_cols:
            if _ != '' and type(_) != float:
                fixed_cols.append(_)
            else:
                if str(fixed_cols[-1]).endswith('_1'):
                    fixed_cols.append(str(fixed_cols[-1]).replace('_1', '_2'))
                else:
                    fixed_cols.append(str(fixed_cols[-1]) + '_1')
        data = data.loc[i+1:]
        data.columns = fixed_cols
    else:
        fixed_cols = []
        for _ in data.columns:
            if 'Unnamed' not in _:
                fixed_cols.append(_)
            else:
                if fixed_cols[-1].endswith('_1'):
                    fixed_cols.append(fixed_cols[-1].replace('_1', '_2'))
                else:
                    fixed_cols.append(fixed_cols[-1] + '_1')
        data.columns = fixed_cols
    data.insert(0, 'ClientUniqueID', cuid)
    if 'Metrics' in data.columns:
        data.drop(columns=['Metrics'], inplace=True)
    return data.fillna(method='ffill').reset_index(drop=True).fillna('')

### Process Agent MPC006 Files ###

def process_agent_file(file):
    data = clean_file('./excel/' + file).drop(columns=['AHT hh:mm:ss', 'ASA mm:ss'])
    FileDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d')).strftime('%Y-%m-%d')
    MetricDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    cols = ['ClientUniqueID', 'Agent', 'SourceSystemName', 'Abandon Rate %',
            'Average Time to Abandon', 'Calls Abandon All', 'Calls Handled',
            'Calls Handled Agent', 'Calls Handled Voicemail', 'Calls Inbound',
            'Calls Offered', 'Total Hold time', 'Total Talk time',
            'Total Work time', 'True Abandons', 'Time to answer',
            'Max Time to Answer', 'Max Time to Abandon', 'Max Talk time',
            'SLA Made %', 'AHT Seconds', 'ASA seconds']
    
    data.columns = cols
    
    data['Abandon Rate %'] = data['Abandon Rate %'].astype(float)
    
    n = 2500
    chunks = [data[i:i+n] for i in range(0, data.shape[0], n)]
    
    for j, chunk in enumerate(chunks):
        print('Processing chunk {} of {}'.format(str(j + 1), str(len(chunks))))
        
        # Create Yattag doc, tag and text objects
        doc, tag, text = Doc().tagtext()
        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doc.asis(xml_header)
        
        _datetime = datetime.utcnow().isoformat()

        with tag('EmployeeMetricInterchange', generated=_datetime):
            for row in chunk.itertuples():
                if row[1] != 'Total':           
                    ClientUniqueID = row[1]
                    Agent = str(int(row[2]))
                    SourceSystemName = row[3]
                    MetricType = 'KPI'
                    for i, x in enumerate(row[4:]):
                        if str(x).lower() != 'nan' and str(x).lower() != '':
                            with tag("EmployeeMetricEvent"):
                                with tag('ClientUniqueID'):
                                    text(ClientUniqueID)
                                with tag('EmployeeSourceSystemID'):
                                    text(Agent)
                                with tag('SourceSystemName'):
                                    text(SourceSystemName)
                                with tag('MetricName'):
                                    text(cols[i + 3])
                                with tag('MetricDate'):
                                    text(MetricDate)
                                with tag('MetricVolume'):
                                    if str(x).lower() == 'nan':
                                        text('')
                                    else:
                                        if type(x) == float:
                                            text(format(x, 'f'))
                                        else:
                                            text(x)
                                with tag('MetricType'):
                                    text(MetricType)

        result = indent(
                        doc.getvalue(),
                        indentation = '	',
                        indent_text = False
                        )
        file_name= 'agent_file_{}__{}_of_{}.xml'.format(FileDate, str(j + 1), str(len(chunks)))
        with open('./xml/{}'.format(file_name),
                  "w", encoding='utf-8') as f:
            f.write(result)
            resp = os.system('curl -X POST -F key={} -F feed=agent -F data=@{}\\xml\\{} {}'.format(key,path,file_name, url))
            print(resp)

### Process SD MPC004 Files  ###

def process_SD_file(file):
    data = clean_file('./excel/' + file).drop(columns=['AHT hh:mm:ss', 'ASA mm:ss'])
    FileDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d')).strftime('%Y-%m-%d')
    MetricDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    cols = ['ClientUniqueID', 'Call Center', 'Site', 'Team', 'Team_1',
            'SourceSystemName', 'Abandon Rate %', 'Average Time to Abandon',
            'Calls Abandon All', 'Calls Handled', 'Calls Handled Agent',
            'Calls Handled Voicemail', 'Calls Inbound', 'Calls Offered',
            'Total Hold time', 'Total Talk time', 'Total Work time',
            'True Abandons', 'Time to answer', 'Max Time to Answer',
            'Max Time to Abandon', 'Max Talk time', 'SLA Made %', 'AHT Seconds',
            'ASA seconds']   
    data.columns = cols   
    # Create Yattag doc, tag and text objects
    doc, tag, text = Doc().tagtext()
    xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
    doc.asis(xml_header)
    _datetime = datetime.utcnow().isoformat()
    with tag('EmployeeMetricInterchange', generated=_datetime):
        for row in data.itertuples():
            if row[1] != 'Total':           
                ClientUniqueID = row[1]
                CallCenter = row[2]
                Site = row[3]
                Team = row[4]
                Team_1 = row[5]
                SourceSystemName = row[6]
                MetricType = 'KPI'
                for i, x in enumerate(row[7:]):
                    if str(x).lower() != 'nan' and str(x).lower() != '':
                        with tag("EmployeeMetricEvent"):
                            with tag('ClientUniqueID'):
                                text(ClientUniqueID)
                            with tag('Center'):
                                text('Service Desk')
                            with tag('QueueName'):
                                text(CallCenter)
                            with tag('Site'):
                                text(Site)
                            with tag('Team'):
                                text(Team)
                            with tag('Team_1'):
                                text(Team_1)
                            with tag('SourceSystemName'):
                                text(SourceSystemName)
                            with tag('MetricName'):
                                text(cols[i + 6])
                            with tag('MetricDate'):
                                text(MetricDate)
                            with tag('MetricVolume'):
                                if str(x).lower() == 'nan':
                                    text('')
                                else:
                                    if type(x) == float:
                                        text(format(x, 'f'))
                                    else:
                                        text(x)
                            with tag('MetricType'):
                                text(MetricType)
                       
    result = indent(
                    doc.getvalue(),
                    indentation = '	',
                    # indentation = '',
                    indent_text = False
                    )
    file_name= 'SD_file_{}.xml'.format(FileDate)
    with open('./xml/{}'.format(file_name),"w", encoding='utf-8') as f:
            f.write(result)
    resp = os.system('curl -X POST -F key={} -F feed=center -F data=@{}\\xml\\{} {}'.format(key,path,file_name,url))
    print(resp)

### Process Productivity MPC003 Files 

def process_productivity_file(file):
    data = clean_file('./excel/' + file).drop(columns=['Team_1','Calls Inbound','Calls Outbound'])
    cols = ['ClientUniqueID', 'Agent','Queue Name', 'SourceSystemName', 'Skill Group', 'Skill Group_1', 'Calls Handled Agent']
    data.columns = cols
    n = 15000
    chunks = [data[i:i+n] for i in range(0, data.shape[0], n)]
    FileDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d')).strftime('%Y-%m-%d')
    MetricDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    for j, chunk in enumerate(chunks):
        print('Processing chunk {} of {}'.format(str(j + 1), str(len(chunks))))
        # Create Yattag doc, tag and text objects
        doc, tag, text = Doc().tagtext()
        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doc.asis(xml_header)
        _datetime = datetime.utcnow().isoformat()
        with tag('EmployeeMetricInterchange', generated=_datetime):
            for row in chunk.itertuples():
                if row[1] != 'Total':           
                    ClientUniqueID = row[1]
                    EmployeeSourceSystemID = row[2]
                    Team = row[3]
                    SourceSystemName = row[4]
                    Skill_Group = row[5]
                    Site = row[6]
                    MetricType = 'Productivity'
                    for i, x in enumerate(row[7:]):
                        if str(x).lower() != 'nan' and str(x).lower() != '':
                            with tag("EmployeeMetricEvent"):
                                with tag('ClientUniqueID'):
                                    text(row[1])
                                with tag('EmployeeSourceSystemID'):
                                    text(EmployeeSourceSystemID)
                                with tag('QueueName'):
                                    text(Team)
                                with tag('SourceSystemName'):
                                    text(SourceSystemName)
                                with tag('MetricName'):
                                    text(Skill_Group)
                                with tag('MetricDate'):
                                    text(MetricDate)
                                with tag('MetricVolume'):
                                    if str(x).lower() == 'nan':
                                        text('')
                                    else:
                                        if type(x) == float:
                                            text(format(x, 'f'))
                                        else:
                                            text(x)
                                with tag('MetricType'):
                                    text(MetricType)
        result = indent(
                        doc.getvalue(),
                        indentation = '	',
                        indent_text = False
                        )
        file_name= 'productivity_file_{}__{}_of_{}.xml'.format(FileDate, str(j + 1), str(len(chunks)))
        with open('./xml/{}'.format(file_name),"w", encoding='utf-8') as f:
            f.write(result)
            resp = os.system('curl -X POST -F key={} -F feed=productivity -F data=@{}\\xml\\{} {}'.format(key,path,file_name,url))
            print(resp)
        
### Process Not Ready MPC007 Files ###

def process_not_ready_file(file):
    data = clean_file('./excel/' + file).drop(columns=['Team', 'Team_1', 'Agent']).drop([0])
    data=data.drop([len(data)])
    data.rename(columns={'Reason code':'Active Time'}, inplace = True)
    cols = data.columns
    n = 2000
    chunks = [data[i:i+n] for i in range(0, data.shape[0], n)]
    FileDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d')).strftime('%Y-%m-%d')
    MetricDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    for j, chunk in enumerate(chunks):
        print('Processing chunk {} of {}'.format(str(j + 1), str(len(chunks))))
        # Create Yattag doc, tag and text objects
        doc, tag, text = Doc().tagtext()
        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doc.asis(xml_header)
        _datetime = datetime.utcnow().isoformat()
        with tag('EmployeeMetricInterchange', generated=_datetime):
            for row in chunk.itertuples():
                if row[1] != 'Total':           
                    ClientUniqueID = row[1]
                    EmployeeSourceSystemID = int(row[2])
                    SourceSystemName = 'Voice'
                    MetricType = 'Hour Type'
                    for i, x in enumerate(row[3:]):
                        if str(x).lower() != 'nan' and str(x).lower() != '':
                            with tag("EmployeeMetricEvent"):
                                with tag('ClientUniqueID'):
                                    text(ClientUniqueID)
                                with tag('SourceSystemName'):
                                    text(SourceSystemName)
                                with tag('EmployeeSourceSystemID'):
                                    text(EmployeeSourceSystemID)
                                with tag('MetricName'):
                                    text(cols[i + 2])
                                with tag('MetricDate'):
                                    text(MetricDate)
                                with tag('MetricVolume'):
                                    if str(x).lower() == 'nan':
                                        text('')
                                    else:
                                        if str(type(x)) == "<class 'datetime.time'>":
                                            ft = datetime.strptime(str(x.hour) + ':' + str(x.minute) + ':' + str(x.second), "%H:%M:%S") - datetime(1900,1,1)
                                            text(format(ft.total_seconds()/86400, '.5f'))
                                        elif str(type(x)) == "<class 'datetime.datetime'>":
                                            ft = datetime.strptime(str(x.year) + "-" + str(x.month) + "-" + str(x.day) + " " + str(x.hour) + ':' + str(x.minute) + ':' +  str(x.second), "%Y-%m-%d %H:%M:%S") - datetime(1900,1,1)                             
                                            text(format((ft.total_seconds()+86400)/86400,'.5f'))
                                        elif type(x) == float:
                                            text(format(x, '.5f'))
                                        elif cols[i + 2] == 'Active Time' and x != '0':
                                            ft = datetime.strptime(x, "%H:%M:%S")- datetime(1900,1,1)
                                            text(format(ft.total_seconds()/86400, '.5f'))
                                        else:
                                            text(x)
                                with tag('MetricType'):
                                    text(MetricType)
        result = indent(
                        doc.getvalue(),
                        indentation = '	',
                        indent_text = False
                        )
        file_name= 'not_ready_file_{}__{}_of_{}.xml'.format(FileDate, str(j + 1), str(len(chunks)))
        with open('./xml/{}'.format(file_name),"w", encoding='utf-8') as f:
            f.write(result)
            resp = os.system('curl -X POST -F key={} -F feed=agent -F data=@{}\\xml\\{} {}'.format(key,path,file_name,url))
            print(resp)

### Process Incident Details MPC008 Files ###

def incident_details(file):
    data = clean_file('./excel/' + file).drop(columns=['Company_1', 'Assigned To', 'Person SK - Assigned To Incidents (1)', 'Resolved by','Closed Date', 'Updated', 'Previous Assigned', 'New Assigned to - Caller', 'Total'])
    data = data.loc[data['Contact Type'].isin(['EMAIL','SELF-SERVICE'])]
    data = data.sort_values(by=['Incident Num','Current Assigned To Change Dt'], ascending = False)
    cols = ['ClientUniqueID', 'Company', 'Incident Num', 'Contact Type',
       'Incident State', 'Resolution Code', 'Resolved by', 'Opened Date',
       'Resolved Date', 'Previous Assigned', 'New assigned to',
       'Current Assigned To Change Dt', 'Opened Measure', 'Resolved Measure']
    data.columns = cols
    n = 15000
    chunks = [data[i:i+n] for i in range(0, data.shape[0], n)]
    inc=''
    FileDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d')).strftime('%Y-%m-%d')
    MetricDate = (datetime.strptime(file[len(file) - 15:len(file)-5],'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    for j, chunk in enumerate(chunks):
        print('Processing chunk {} of {}'.format(str(j + 1), str(len(chunks))))
        # Create Yattag doc, tag and text objects
        doc, tag, text = Doc().tagtext()
        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doc.asis(xml_header)
        _datetime = datetime.utcnow().isoformat()
        with tag('EmployeeMetricInterchange', generated=_datetime):
            for row in chunk.itertuples():
                if row[1] != 'Total' and inc != row[3]:          
                    ClientUniqueID = row[1]
                    if row[7] == '0':
                        EmployeeSourceSystemID = row[10]
                    else:
                        EmployeeSourceSystemID = row[7]
                    Team = row[2]
                    SourceSystemName = '{} TICKET'.format(row[4])
                    MetricType = 'Productivity'
                    for i, x in enumerate(row[13:]):
                        if str(x).lower() != 'nan' and str(x).lower() != '':
                            if cols[i + 12] == 'Opened Measure' or (cols[i + 12] == 'Resolved Measure' and row[7]!= '0'):
                                with tag("EmployeeMetricEvent"):
                                    with tag('ClientUniqueID'):
                                        text(ClientUniqueID)
                                    with tag('EmployeeSourceSystemID'):
                                        if cols[i + 12] == 'Opened Measure':
                                            text(row[10])
                                        else:
                                            text(row[7])
                                    with tag('QueueName'):
                                        text(Team)
                                    with tag('SourceSystemName'):
                                        text(SourceSystemName)
                                    with tag('ticket_number'):
                                        text(row[3])
                                    with tag('opened_date'):
                                        text(row[8].strftime("%Y-%m-%d %H:%M:%S"))
                                    with tag('resolved_date'):
                                        if str(type(row[9])) == "<class 'datetime.datetime'>":
                                            text(row[9].strftime("%Y-%m-%d %H:%M:%S"))
                                        else:
                                            text(0)  
                                    with tag('MetricName'):
                                        if cols[i + 12] == 'Opened Measure':
                                            text('ticket_assigned')
                                        else:
                                            text('ticket_resolved')
                                    with tag('MetricDate'):
                                        text(MetricDate)
                                    with tag('MetricVolume'):
                                        if cols[i + 12] == 'Opened Measure':
                                            text(row[13])
                                        else:
                                            text(row[14])
                                    with tag('MetricType'):
                                        text(MetricType)
                    inc = row[3]  
        result = indent(
                        doc.getvalue(),
                        indentation = '	',
                        indent_text = False
                        )
        file_name= 'incident_details_file_{}__{}_of_{}.xml'.format(FileDate, str(j + 1), str(len(chunks)))
        with open('./xml/{}'.format(file_name),"w", encoding='utf-8') as f:
            f.write(result)
            resp = os.system('curl -X POST -F key={} -F feed=productivity -F data=@{}\\xml\\{} {}'.format(key,path,file_name,url))
            print(resp)
