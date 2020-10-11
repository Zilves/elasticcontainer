#!/usr/bin/env python3
import sys
import getpass
import logging
from datetime import timedelta
from utils import database
from classes.request import Request
from classes.user import User
from classes.container import Container
from classes.application import Application


class Session:
    uid = -1
    reqid = -1


def loginMenu(session):
    print("""
    1. Login
    2. Create a user
    3. Exit
    """)
    menu = input('Enter the Selected Option: ')

    if menu == '1':
        login = input('Login: ')
        password = getpass.getpass(prompt='Password: ')
        uid = database.check_login(login, password)

        if not uid:
            print('Wrong Login and/or Password! User Not Found!')
        else:
            print("Login success!")
            session.uid = uid

    elif menu == '2':
        user = User()
        user.name = input('Enter your Complete Name: ')
        user.login = input('Enter your Login: ')

        while True:
            user.password = getpass.getpass(prompt='Password: ')
            checkpass = getpass.getpass(prompt='Repeat Password: ')

            if user.password != checkpass:
                print('The Passwords mismatch! Rewrite the Password.')
            else:
                session.uid = database.create_user(user)
                break

    elif menu == "3":
        print("Get Out...")
        sys.exit()

    else:
        print('Invalid Option!')

    # print('User ID: ', session.uid)


def applicationMenu():
    application_list = database.list_applications()

    if application_list:
        print('\n' + 'Applications:')

        for app in application_list:
            print('ID: ', app.appid, ' Name: ', app.name)

        while True:
            application = Application()
            application.appid = int(input('Enter the Selected Application ID: '))
            if application in application_list:
                return(application.appid)
                # break
            else:
                print('ID: ', application.appid, ' - Wrong Application ID!')
    else:
        print('No Applications Available!')


def requestMenu(session):
    print("""
    1. Create a New Request
    2. Exit
    """)
    menu = input("Enter the Selected Option: ")

    if menu == '1':
        request = Request()
        print('\n')
        request.name = input('Enter Request Name: ')
        request.user = session.uid
        request.num_containers = int(input('Enter the Number of Containers: '))

        print("""
        1. Executing equal jobs?
        2. Executing different jobs?
        3. Exit
        """)
        menu2 = input('Enter the Selected Option: ')

        if menu2 == '1':
            request.listcontainers = containerMenuType2(session, request.num_containers)

        elif menu2 == '2':
            request.listcontainers = containerMenuType1(session, request.num_containers)

        elif menu2 == '3':
            print('Get Out...')
            sys.exit()

        else:
            print('Invalid Option!')

        session.reqid = database.create_request(request)

        print('Request ID: ', session.reqid)
        i = 0

        for container in request.listcontainers:
            container.name = 'rqst' + str(session.reqid) + 'cntnr' + str(i)
            database.create_container(session.reqid, container.appid, container.name, container.command, container.estimated_time)
            i += 1

    elif menu == '2':
        print('Get Out...')
        sys.exit()

    else:
        print('Invalid Option!')

def containerMenuType1(session, qtd_containers):
    container_list = []
    print('Containers with different jobs')

    for i in range(qtd_containers):
        print('\n' + 'Container:', i)
        container = Container()
        container.appid = applicationMenu()
        container.command = input('Enter Needed Execution Command: ')
        container.estimated_time = timedelta(seconds = int(input('Enter Stimated Execution Time in seconds: ')))
        print('Estimated Time = ', container.estimated_time)
        container_list.append(container)
        # database.create_container(session.reqid, appid, name, command, est_time)

    return container_list

def containerMenuType2(session, qtd_containers):
    container_list = []
    print('Containers with equal jobs')

    appid = applicationMenu()
    command = input('Enter Needed Execution Command: ')
    estimated_time = timedelta(seconds = int(input('Enter Stimated Execution Time in seconds: ')))
    print('Estimated Time = ', estimated_time)

    for i in range(qtd_containers):
        print('\n' + 'Container:', i)
        container = Container()
        container.appid = appid
        container.command = command
        container.estimated_time = estimated_time
        container_list.append(container)
        # database.create_container(session.reqid, appid, name, command, est_time)

    return container_list


# ----------- Script Principal ----------


if __name__ == '__main__':
    logging.basicConfig(filename='./log/submit-script.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S',level=logging.INFO)

    session = Session()

    while True:
        loginMenu(session)

        if session.uid != -1:
            break

    while True:
        requestMenu(session)
