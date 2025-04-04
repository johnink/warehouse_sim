import pyodbc
import pandas as pd
import simpy
import math
import datetime

filepath = r"*output path*\output.xlsx"

start_date = '2024-03-22'
end_date = '2024-03-23'
run_for = 1440
#1440 one day

#configure employee count
planners = 10
pickers = 20
movers = 10
packers = 32

#configure times
plan_minutes = 0.02/60
pick_minutes = 2
replen_minutes = 80/60
packout_minutes = 50/60
vasunits_minutes = 11.3/60
vaspackage_minutes = 45/60
percent_vas = .75

replen_amount = 48


"""
SET UP DATAFRAMES
"""
#for the tab in Excel
#so in the file you can tell the configuration at the time the script was run
data = {"config_name":["start_date","end_date","planners","pickers","movers","packers","plan_minutes","pick_minutes","replen_minutes",
                       "packout_minutes","vasunits_minutes","vaspackage_minutes","replen_amount"],
        "setting":[start_date, end_date,planners,pickers,movers,packers,plan_minutes,pick_minutes,replen_minutes,
                   packout_minutes,vasunits_minutes,vaspackage_minutes,replen_amount] 
        }
config_df = pd.DataFrame(data)

#set up the cart que
data = {"rid":[],"box_id":[], "sku":[], "sent":[]}
cartque_df = pd.DataFrame(data)


"""
DATAPULLS FROM SERVER
"""
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=*SERVER*')

cl = pd.read_sql_query("""
    SELECT row_number() OVER (ORDER BY datetime_create, cnt.sku) AS rid
        , datetime_create
        , channel
        , shipping_method
        , DATEDIFF(minute, ?, datetime_create) AS minute_create
    FROM *CONTAINER TABLE* AS cnt
    WHERE datetime_create >= ?
         AND datetime_create <  ?
    """,conn, params=[start_date, start_date, end_date])



dummyslot_df = pd.read_sql_query("""
    SELECT sku
	         , units
	         , 0 AS demand
	         , 0 AS intransit
             , 0 replen_time
    FROM *SLOTTING TABLE* AS cnt
    WHERE datetime_create >= ?
    AND datetime_create <  ?
    """,conn, params=[start_date, end_date])


"""
ADDING FIELDS
"""
cl["cart_id"] = pd.NA

cl["plan_time"] = pd.NA
cl["pick_time"] = pd.NA
cl["replen_time"] = pd.NA
cl["ready_time"] = pd.NA
cl["packout_time"] = pd.NA
cl["complete_time"] = pd.NA

cl["plan_elapsed"] = pd.NA
cl["pick_elapsed"] = pd.NA
cl["replen_elapsed"] = pd.NA
cl["packout_elapsed"] = pd.NA

cl["plan_delay"] = pd.NA
cl["pick_delay"] = pd.NA
cl["replen_delay"] = pd.NA
cl["packout_delay"] = pd.NA

cl["plan_utilization"] = pd.NA
cl["pick_utilization"] = pd.NA
cl["replen_utilization"] = pd.NA
cl["packout_utilization"] = pd.NA

"""
DECLARE FUNCTIONS
"""

def minute_to_time(minutestamp):
    return datetime.datetime.strptime(start_date,"%Y-%m-%d") + pd.to_timedelta(minutestamp, unit='m')

def set_cl_timestamp(column_type, filter, filter_value, set_minutes, set_elapsed, set_delay, set_utilization):
    cl.loc[cl[filter]==filter_value,column_type + "_time"] = minute_to_time(set_minutes)
    cl.loc[cl[filter]==filter_value,column_type + "_elapsed"] = set_elapsed
    cl.loc[cl[filter]==filter_value,column_type + "_delay"] = set_delay
    cl.loc[cl[filter]==filter_value,column_type + "_utilization"] = set_utilization

def calc_packout_time( channel, units):
        total_minutes = (packout_minutes * units) 
        if channel == 'VAS':
            total_minutes = total_minutes + ((vasunits_minutes * units) * percent_vas) + (vaspackage_minutes)
        return total_minutes


def focus_df(transact):
    t = cl[cl[transact + "_time"].notna()][["channel","shipping_method","order_id","sku","qty","box_id","box_type",transact + "_time",transact + "_elapsed",transact + "_delay",transact + "_utilization"]]
    t.insert(loc=0, column='transact', value=transact)
    t.rename(columns={transact + '_time': 'transact_time'}, inplace=True)
    t.rename(columns={transact + '_elapsed': 'transact_elapsed'}, inplace=True)
    t.rename(columns={transact + '_delay': 'transact_delay'}, inplace=True)
    t.rename(columns={transact + '_utilization': 'transact_utilization'}, inplace=True)
    return t

"""
Warehouse initiate
"""

class Warehouse(object):
    def __init__(self, env, num_planners, num_picker, num_materialhandler, num_packers):
        self.env = env
        self.planners = simpy.Resource(env, num_planners)
        self.picking = simpy.Resource(env, num_picker)
        self.materialhandling = simpy.Resource(env, num_materialhandler)
        self.packing = simpy.Resource(env, num_packers)

    def plan_product(self):
        yield self.env.timeout(plan_minutes)
        
    def move_product(self):
        yield self.env.timeout(replen_minutes)

    def pick_product(self):
        yield self.env.timeout((pick_minutes * 12) - 1)

    def pack_out(self, total_minutes):
        yield self.env.timeout(total_minutes)


"""
PROCESS ORDER
This is where every order is started. 
"""

def run_order(env, carton_line, warehouse, cartons, order_id):
    #PLANNING
    #plan the order
    start = env.now
    with warehouse.planners.request() as request:
        yield request
        yield env.process(warehouse.plan_product(carton_line))
    elapsed = env.now - start
    delay = elapsed - plan_minutes
    utilization = (warehouse.planners.count + 1) / warehouse.planners.capacity
    set_cl_timestamp('plan','order_id',order_id,env.now,elapsed,delay, utilization )

    for index, row in cartons.iterrows():
        env.process(plan_line(env, carton_line, warehouse, row))
    
    #wait for bin picking and replenishments
    while cl.loc[(cl["order_id"] == order_id) & (cl["ready_time"].isna()), "rid"].any().any():
        yield env.timeout(.5)

    for box in cartons["box_id"].unique():
        box_lines = cartons[cartons["box_id"]==box]
        env.process(process_container(env, carton_line, warehouse, box_lines, box))


"""
PICK AND REPLEN
"""
def plan_line(env, carton_line, warehouse, row):    
    start = env.now
    #kick off replenishments, if needed
    #add the units on the order to the demand
    dummyslot_df.loc[dummyslot_df["sku"] == row["sku"],"demand"] += row["qty"]

    #if the demand is higher than the units in location, kick off a replenshment
    if dummyslot_df[dummyslot_df["sku"] == row["sku"]]["demand"].item() > dummyslot_df[dummyslot_df["sku"] == row["sku"]]["units"].item() + dummyslot_df[dummyslot_df["sku"] == row["sku"]]["intransit"].item():
        dummyslot_df.loc[dummyslot_df["sku"] == row["sku"], "intransit"] += replen_amount
        with warehouse.materialhandling.request() as request:
            yield request
            yield env.process(warehouse.move_product(carton_line))
        #set timestams in the dataframe, for export to excel
        elapsed = env.now - start
        delay = elapsed - (replen_minutes)
        utilization = warehouse.materialhandling.count / warehouse.materialhandling.capacity
        set_cl_timestamp('replen','rid',row["rid"], env.now,elapsed, delay, utilization)
        dummyslot_df.loc[dummyslot_df["sku"] == row["sku"], "intransit"] -= replen_amount
        dummyslot_df.loc[dummyslot_df["sku"] == row["sku"], "units"] += replen_amount
        dummyslot_df.loc[dummyslot_df["sku"] == row["sku"], "replen_time"] = minute_to_time(env.now)

    #if an item shows intransit quantity, wait until replen is complete.
    elif dummyslot_df[dummyslot_df["sku"] == row["sku"]]["intransit"].item() > 0:
        while dummyslot_df[dummyslot_df["sku"] == row["sku"]]["intransit"].item() > 0:
            yield env.timeout(.5)
        elapsed = env.now - start
        delay = elapsed - (replen_minutes)
        utilization = warehouse.materialhandling.count / warehouse.materialhandling.capacity
        set_cl_timestamp('replen','rid',row["rid"], env.now,elapsed, delay, utilization)
        dummyslot_df.loc[dummyslot_df["sku"] == row["sku"], "replen_time"] = minute_to_time(env.now)

    #adds the cart to the cart que, and waits for the cart to be complete
    cartque_df.loc[row["rid"]] = [row["rid"], row["box_id"], row["sku"], False]
    while cartque_df[cartque_df["rid"] == row["rid"]].any().any():
        yield env.timeout(.5)
    #sets the pick time
    elapsed = env.now - start
    delay = elapsed - (pick_minutes)
    utilization = warehouse.picking.count / warehouse.picking.capacity
    set_cl_timestamp('pick','rid',row["rid"],env.now,elapsed, delay, utilization)
    

    cl.loc[cl["rid"]==row["rid"],"ready_time"] = minute_to_time(env.now)



"""
PICKING CART QUE
"""

def cart_builder(env, carton_line, warehouse):
    #when the cart que has enough orders for a new cart, send it on its way
    while cartque_df[cartque_df["sent"] == False]["rid"].nunique() > 12:
        cart = cartque_df[cartque_df["sent"] == False]["rid"].unique()[0:12]
        cartque_df.loc[cartque_df["rid"].isin(cart), "sent"] = True
        env.process(send_cart(env, carton_line, warehouse, cart))

def send_cart(env, carton_line, warehouse, cart):
    #simulates a picker going around and picking
    with warehouse.picking.request() as request:
        yield request
        yield env.process(warehouse.pick_product(carton_line)) 
    #getting the pick_count for pick delay   
    cl.loc[cl["rid"].isin(cart), "cart_id"] = cart[0]
    cartque_df.drop(cartque_df.loc[cartque_df["rid"].isin(cart), "sent"].index, axis='index', inplace=True)
    


"""
PACKOUT
"""

def process_container(env, carton_line, warehouse, box_lines, box):
    start=env.now
    box_channel = box_lines["channel"].max()
    box_units = box_lines["qty"].sum()
    packout_minutes = calc_packout_time(box_channel, box_units)
    with warehouse.packing.request() as request:
        yield request
        yield env.process(warehouse.pack_out(carton_line, packout_minutes))
    elapsed=env.now-start
    delay = elapsed - packout_minutes
    utilization = (warehouse.packing.count + 1) / warehouse.packing.capacity
    set_cl_timestamp('packout','box_id',box,env.now,elapsed, delay, utilization)
    if cl.loc[cl["order_id"] == box_lines["order_id"].max(),"packout_time"].notna().any():
        cl.loc[cl["order_id"] == box_lines["order_id"].max(),"complete_time"] = minute_to_time(env.now)


    

"""
WAREHOUSE
runs orders
"""

def run_warehouse(env, num_planners, num_picker, num_materialhandler, num_packers):
    warehouse = Warehouse(env, num_planners, num_picker, num_materialhandler, num_packers)

    carton_line = 0
    
    #run until end time is hit.
    while True:
        cartons = cl[cl["minute_create"] == round(env.now)]
        carton_line += cartons.count()
        cart_builder(env, carton_line, warehouse)
        for order in cartons["order_id"].unique():
            env.process(run_order(env, carton_line, warehouse, cartons[cartons["order_id"]==order], order))
        yield env.timeout(1)
          
"""
ENVIRONMENT START
Runs warehouse.
"""

env = simpy.Environment()  

env.process(run_warehouse(env, planners, pickers, movers, packers))
env.run(until=run_for)


"""
MANIPULATE DATAFRAMES
Once the simulation is complete, manipulate the resulting dataframes in preperation for export
"""

config_df.set_index("config_name",inplace=True)
cl.set_index("rid",inplace=True)
dummyslot_df.set_index("sku",inplace=True)
# unpivot times
clup = focus_df('plan')
t = focus_df('pick')
clup = pd.concat([clup, t])
t = focus_df('replen')
clup = pd.concat([clup, t])
t = focus_df('packout')
clup = pd.concat([clup, t])

#export to excel sheets
with pd.ExcelWriter(filepath) as writer:
    config_df.to_excel(writer, sheet_name="config")
    clup.to_excel(writer, sheet_name='transact')
    cl.to_excel(writer, sheet_name='contlines_raw')
    dummyslot_df.to_excel(writer, sheet_name='dummyslot_raw')
