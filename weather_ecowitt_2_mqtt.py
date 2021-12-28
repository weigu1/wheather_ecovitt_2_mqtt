#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
###############################################################################
#
#  Name:         weather_ecowitt_2_mqtt.py
#  Purpose:      Get the ecowitt weather data on port 8000 and send data to
#                the mqtt server
#  Author:       weigu.lu
#  Date:         2021-12-28
#  Version       1.0.1
#
#  Copyright 2020 weigu <weigu@weigu.lu>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
###############################################################################
"""
# pylint: disable-msg=C0103
# pylint: disable-msg=R0912
# pylint: disable-msg=R0914
# pylint: disable-msg=R0915
# pylint: disable-msg=W0105

import socket
import sys
from time import strftime, localtime
from datetime import datetime
import paho.mqtt.client as mqtt


HOST = ''   # empty: all interfaces
PORT = 8000

# MQTT things
MQTT_CLIENT_ID = "weatherstation_" + strftime("%s", localtime()) # must be unique!# must be unique!
MQTT_SERVER_IP = "192.168.1.60" # 127.0.0.1 if mosquitto is running on same server as python script
MQTT_SERVER_PORT = 1883
MQTT_TOPIC = "weigu/garden/garden/weatherstation"

#-------------------------------------------------
def on_connect(_client, _userdata, _flags, result_code):
    """Callback that is executed when the client receives a CONNACK response
       from the server. Subscribe to the topic (topic name, QoS)."""
    print("Connected with result code "+str(result_code))
    mqtt_client.subscribe(MQTT_TOPIC, 0)

def on_disconnect(_client, _userdata, _message):
    """Callback that is executed when we disconnect from the broker."""
    print("Disconnected from the broker.")


def on_subscribe(_client, _userdata, _mid, _granted_qos):
    """Callback that is executed when subscribing to a topic."""
    print('Subscribed on topic.')


def on_unsubscribe(_client, _userdata, _mid, _granted_qos):
    """Callback that is executed when unsubscribing to a topic."""
    print('Unsubscribed on topic.')

def on_message(_client, _userdata, message):
    """Callback that is executed when a message is received."""
    print('Message received.')

def parse_ecowitt_data(data):
    """Parse the ecowitt data and add it to a dictionnary."""
    data_dict = {}          # create empty dictionary
    data_list = data.decode().split('&')
    #print(data_list)
    for item in data_list:
        if item.find("dateutc") != -1:
            ws_datetime = item[item.find('=')+1:].replace('+', 'T')
            data_dict['datetime_utc'] = ws_datetime
        if item.find("tempinf") != -1:
            temp_in = round((float(item[item.find('=')+1:])-32.0) * 5/9, 2) #from F to C
            data_dict['Temperature_in_[C]'] = temp_in
        if item.find("humidityin") != -1:
            hum_in = int(item[item.find('=')+1:])
            data_dict['Humidity_in_[%]'] = hum_in
        if item.find("baromrelin") != -1:
            barometer_rel_in = round((float(item[item.find('=')+1:])*33.86389), 2) #inch_mercury-hpa
            data_dict['Barometer_rel_in_[hpa]'] = barometer_rel_in
        if item.find("baromabsin") != -1:
            barometer_abs_in = round((float(item[item.find('=')+1:])*33.86389), 2) #inch_mercury-hpa
            data_dict['Barometer_abs_in_[hpa]'] = barometer_abs_in
        if item.find("tempf") != -1:
            temp_out = round((float(item[item.find('=')+1:])-32.0) * 5/9, 2)
            data_dict['Temperature_out_[C]'] = temp_out
        if item.find("humidity") != -1:
            hum_out = int(item[item.find('=')+1:])
            data_dict['Humidity_out_[%]'] = hum_out
        if item.find("winddir") != -1:
            wind_dir = int(item[item.find('=')+1:])
            data_dict['Wind_direction_[degree]'] = wind_dir
        if item.find("windspeedmph") != -1:
            wind_speed = round(float(item[item.find('=')+1:])*1.609344, 2)
            data_dict['Wind_speed_[km/h]'] = wind_speed
        if item.find("windgustmph") != -1:
            wind_gust_speed = round(float(item[item.find('=')+1:])*1.609344, 2)
            data_dict['Wind_gust_speed_[km/h]'] = wind_gust_speed
        if item.find("rainratein") != -1:
            rain_rate_in = float(item[item.find('=')+1:])
            data_dict['Rain_rate_[]'] = rain_rate_in
        if item.find("eventrainin") != -1:
            rain_event_in = float(item[item.find('=')+1:])
            data_dict['Rain_event_[]'] = rain_event_in
        if item.find("hourlyrainin") != -1:
            rain_hourly_in = float(item[item.find('=')+1:])
            data_dict['Rain_hourly_[]'] = rain_hourly_in
        if item.find("dailyrainin") != -1:
            rain_daily_in = float(item[item.find('=')+1:])
            data_dict['Rain_daily_[]'] = rain_daily_in
        if item.find("weeklyrainin") != -1:
            rain_weekly_in = float(item[item.find('=')+1:])
            data_dict['Rain_weekly'] = rain_weekly_in
        if item.find("monthlyrainin") != -1:
            wind_gust_speed = float(item[item.find('=')+1:])
            data_dict['Rain_monthly_[]'] = wind_gust_speed
        if item.find("totalrainin") != -1:
            wind_gust_speed = float(item[item.find('=')+1:])
            data_dict['Rain_total_[]'] = wind_gust_speed
        if item.find("solarradiation") != -1:
            solar_radiation = float(item[item.find('=')+1:])
            data_dict['Solar_radiation_[W/m^2]'] = solar_radiation
        if item.find("uv") != -1:
            uv_radiation = float(item[item.find('=')+1:])
            data_dict['UV_[km/h]'] = uv_radiation
    # Calculate own rel baro from height
    hasl = 240 # Height above sea level in m
    aap = barometer_abs_in
    atc = temp_out
    baro_rel_in_calc = round(aap + ((aap * 9.80665 * hasl)/(287 * (273 + atc + (hasl/400)))), 2)
    data_dict['Barometer_rel_in_calc_[hpa]'] = baro_rel_in_calc
    return data_dict

#---- MAIN --------------------------------------------------------------------

mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)

def main():
    """main"""
    print("Program started at ", datetime.now().isoformat('T', 'seconds'))
    # define the callback functions
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_subscribe = on_subscribe
    mqtt_client.on_unsubscribe = on_unsubscribe
    mqtt_client.on_message = on_message
    # connect to the broker
    mqtt_client.connect(MQTT_SERVER_IP, MQTT_SERVER_PORT, keepalive=60,
                        bind_address="")
    # start loop to process callbacks! (new thread!)
    mqtt_client.loop_start()

    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')
    try:
        my_socket.bind((HOST, PORT))
    except socket.error:
        print('Binding failed')
        sys.exit()
    print('Binding socket completed')
    my_socket.listen(3) # listening on socket (incoming connections we're willing to queue)
    print('Listening on socket')

    try:
        while True:
            now = datetime.now()
            now_time = now.time()
            print(now_time)
            conn, addr = my_socket.accept() # wait for connection (blocking call)
            with conn:
                print('Connected with ' + addr[0] + ':' + str(addr[1]))
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    #print(data)                    
                    message = parse_ecowitt_data(data)
                    message = str(message).replace("'", '\"')
                    print(message)
                    mqtt_client.publish(MQTT_TOPIC, message, qos=0, retain=False)
        my_socket.close()
    except KeyboardInterrupt:
        print("Keyboard interrupt by user")
        mqtt_client.loop_stop() # clean up
        mqtt_client.disconnect()
        my_socket.close()

main()
