

CREATE TABLE IF NOT EXISTS user_profile (k text PRIMARY KEY, v text, id bigserial not null,
           userid          bigint not null default 928732,
              ismasterprofile bigint not null default '0',
             profiletype     varchar(255) default 'application/pdf',
             orderid         bigint default '4645',
            name            varchar(255) default 'Jaymie Bullick',
              locale          varchar(255),
              profilepic      varchar(255) default 'http://dummyimage.com/213x100.png/ff4444/ffffff',
              createdby       bigint default 33956,
              createddate     timestamptz default '2018-10-15T12:10:21Z',
              updatedby       bigint default 18315,
              updateddate     timestamptz default '2018-03-14T02:22:03Z',
              isdeleted       boolean default 'f') split into 40 tablets;


CREATE TABLE IF NOT EXISTS api_watch_list(
           k                       text PRIMARY KEY,
             v                       text ,
            id                      bigserial not null,
           userid                  bigint not null default '402136',
             profileid               bigint not null default '913657',
              externalid              varchar(255) not null default '7f985532-4f86-43d7-99b4-fbb2bd3c3609',
              watchlistexternalidtype varchar(255) default 'fa7cf363-8e0a-454a-a39c-3e1df3633c47',
             createddate             timestamptz default '2020-10-03T07:35:35Z',
              updateddate             timestamptz default '2013-04-29T08:36:50Z') split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_user_partner_transaction_log(
              k               text PRIMARY KEY,
              v               text ,
            id              bigserial not null,
             isoriginalpurchase    boolean default false,
              json                  text default '[{},{},{},{},{}]',
              originaltransactionid varchar(255) default '8a85eed2-acf5-49b3-bb41-18a06d2dbd4a',
             partner               varchar(255) default 'Kayveo',
              transactionid         varchar(255) default '3db4d92b-920b-4f82-a50c-a656993cf02d',
             transactiontype       bigint default 444,
            userid                bigint default 479818) split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_user_device (
          k                 text PRIMARY KEY,
            v                 text ,
            id                bigserial not null,
            activationcode    varchar(255) default 'activationCode2',
            createddate       timestamptz default '2012-06-29T01:45:58Z',
             devicedescription varchar(255) default 'Damaliscus dorcas',
             deviceid          varchar(255) default '56062-535',
            devicetoken       varchar(255) default 'c6687952-46c1-4669-9817-1a0c605206d3',
            ipaddress         varchar(255) default '56.149.207.111',
            partner           varchar(255) default 'partner3',
            lastupdate        timestamptz default '2021-07-22T20:12:29Z',
            status            bigint default 80,
             userid            bigint default 8590,
            encryption_level  bigint default 6,
            isdeleted         boolean default false) split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_sub_user_packages(
          k                                   text PRIMARY KEY,
              v                                   text ,
             id                                  bigserial not null,
            createddate                         timestamptz default '2011-01-25T05:04:12Z',
             enddate                             timestamptz default '2015-05-26T19:10:05Z',
              packagecode                         varchar(255) default 'package1',
             pspmodifieddate                     timestamptz default '2013-02-08T23:37:25Z',
             state                               bigint default 54,
             updateddate                         timestamptz default '2010-10-19T17:06:48Z',
              userid                              bigint default 704557,
              source                              varchar(255) default 'Aquila chrysaetos',
             couponused                          varchar(255) default 'attitude',
              isstateactive                       bigint default 54,
              cbsproductcode                      varchar(255) default 'Ford',
              vendororiginalpurchasetransactionid varchar(255) default '3532442447867958',
              vendorsuppliedid                    varchar(255) default '17NvYwZ6H8EBwahPoJYnGCMyYLb9hLEUoq',
              lastbillingvendorsynctimestamp      bigint default 4872013,
              expirationintent                    varchar(255) default 'dignissim vestibulum vestibulum ante ipsum primis',
              renewstatus                         varchar(255) default 'open',
              retryvalue                          varchar(255) default 'Formaldehyde',
              cancelreason                        varchar(255) default 'nulla tempus vivamus in felis eu sapien cursus vestibulum proin',
              product_region                      varchar(255) default 'Saint Vincent and the Grenadines',
              startdate                           timestamptz default '2021-12-24T13:28:10Z',
              vendorcode                          varchar(255) default '57520-0802') split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_sub_recurly_notify_log(
            k               text PRIMARY KEY,
             v               text ,
           id              bigserial not null,
            ingestdate      timestamptz default '2020-09-03T08:21:22Z',
           notification   varchar(255) default 'Polarised multi-state interface',
            subscriptionid varchar(255) default 1307,
            userid         bigint default 324586) split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_oauth_access_token(
             k                      text PRIMARY KEY,
             v                      text ,
             access_token_id        bigserial not null,
            access_token           varchar(2048) not null default '15eujNJCmhVdqFN5T3dPkGEh6hfo3kTbSn',
             access_token_hashed    varchar(512) not null default '8bad98be96213362ff4aa8c638f0e0c4c1aa2ba032c86de7ceab2c38d9d1986f',
             additional_information varchar(1024) default 'Curabitur at ipsum ac tellus semper interdum.',
              client_id              varchar(255) not null default 'f0b015f4-673f-4ca3-9e2a-ecc0aec3d9c7',
              expires_in             timestamptz not null default '2013-11-16T04:40:29Z',
              grant_type             bigint not null default 56,
             refresh_token          varchar(2048) not null default '1FhDfbBiBVXWGZfXTG6AhYEtte7CDmpd4y',
              refresh_token_hashed   varchar(512) not null default '30799c54ca6547a63a5cec4afe2a294c039a028e46038e93d6dcd1bbdda62c4f',
              scope                  bigint default 95,
              userid                 bigint not null default 139238) split into 40 tablets;



CREATE TABLE IF NOT EXISTS api_apple_store_receipt_log(
            k               text PRIMARY KEY,
             v               text ,
            id              bigserial not null,
            appitemid       varchar(255) default 'fa4323d6-8293-46e6-b0eb-a080cfeb04cf',
            bid             varchar(255) default  '917e7638-ea19-4d55-91a9-d41cfd3de13a',
            bvrs            varchar(255) default  'Portuguese',
              expiresdate       varchar(255) default '2010-06-23T12:53:53Z',
             expiresdateformated       varchar(255) default 'YYYY-MM-ddTHH:mm:ssZ',
             expiresdateformattedpst       varchar(255) default 'YYYY/dd/mm',
             itemid       varchar(255) default '1f17b20c-cf72-44c2-99ae-eac91bb15c97',
             originalpurchasedate       varchar(255) default '2021-05-18T17:03:32Z',
            originalpurchasedatems       varchar(255) default 'YYYY-MM-ddTHH:mm:ssZ',
            originalpurchasedatepst       varchar(255) default '2010-09-16',
            originaltransactionid       varchar(255) default '7296ec22-3ef7-4474-b7e5-df720ec1bb5e',
            productid       varchar(255) default '1G6AF5S36E0511228',
             purchasedate       varchar(255) default '2019-11-18T15:44:46Z',
            purchasedatems       varchar(255) default 'YYYY-MM-ddTHH:mm:ssZ',
             purchasedatepst       varchar(255) default '2012-02-28',
             quantity       bigint not null default '67',
            transactionid       varchar(255) default '45b08c1d-f9ef-4127-80c8-a65f049713d2',
            uniqueidentifier       varchar(255) default 'd511accb-99f1-4397-9693-ca6802da2760',
             userid       bigint default '260036',
             versionexternalidentifier       varchar(255) default 'fe6bde4f-69c6-406c-91ee-e24f281535df',
             weborderlineitemid       varchar(255) default '00cb6627-91c7-4aa3-ad2d-ead96011eb5c',
            expirationintent       bigint default '21',
            cancellationreason       bigint default '84') split into 40 tablets;

CREATE TABLE IF NOT EXISTS api_sub_apple_orig_transactions(
            k                     text PRIMARY KEY,
              v                     text ,
              id                    bigserial not null,
              createddate           timestamptz default '2016-07-14T09:39:04Z',
              lastcheckeddate       timestamptz default '2019-02-20T13:44:56Z',
              originaltransactionid varchar(255) default '39e64acc-1c47-434a-9066-96abe8ba7add',
             receipttext           text default 'Azeri',
            status                bigint default 79) split into 40 tablets;




