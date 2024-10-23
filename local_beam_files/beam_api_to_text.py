import apache_beam as beam
from apache_beam.transforms import window
from datetime import datetime
import pytz
import json


class AddWindowStartFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        yield (str(window.end.to_utc_datetime()), element)

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

def decoder(message_bytes):
    message_list=message_bytes.decode("utf-8")
    message_str=message_list.strip("[]")
    message_json=json.loads(message_str)
    return message_json


class Counter(beam.DoFn):
    def process(self, group_with_key):
        group=group_with_key[1]
        key=group_with_key[0]
        count=len(group)
        number_of_male=0
        total_age=0
        total_register_age=0
        for i in group:
            if i["gender"]=="male":
                number_of_male=number_of_male+1
            total_age=total_age+i['dob']['age']
            total_register_age=total_register_age+i['registered']['age']
        

        number_of_female=count-number_of_male
        avg_age=total_age/count
        avg_reg_age=total_register_age/count


        yield {"timestamp":key,
               "Number_of_customer":count,
               "number_of_male":number_of_male,
               "number_of_female":number_of_female,
               "avg_customer_age":avg_age,
               "avg_customer_registred_age":avg_reg_age}
        

with beam.Pipeline() as p:


    events = [
        window.TimestampedValue(b'[{"gender": "female", "name": {"title": "Miss", "first": "\\u0622\\u0631\\u0645\\u06cc\\u062a\\u0627", "last": "\\u06cc\\u0627\\u0633\\u0645\\u06cc"}, "location": {"street": {"number": 1044, "name": "\\u062c\\u0645\\u0647\\u0648\\u0631\\u06cc \\u0627\\u0633\\u0644\\u0627\\u0645\\u06cc"}, "city": "\\u0628\\u0648\\u0634\\u0647\\u0631", "state": "\\u0627\\u06cc\\u0644\\u0627\\u0645", "country": "Iran", "postcode": 25457, "coordinates": {"latitude": "16.2200", "longitude": "12.0412"}, "timezone": {"offset": "+1:00", "description": "Brussels, Copenhagen, Madrid, Paris"}}, "email": "armyt.ysmy@example.com", "login": {"uuid": "0f171342-2987-4ca6-8fd4-63e7e6be8a35", "username": "crazygorilla302", "password": "skyler", "salt": "KFEXQBW0", "md5": "a06e0a17ffb7c424861cc02fd06983b9", "sha1": "ea20931ef7d744f804f5a1d25bdfde6f1695d9fc", "sha256": "903ca93074778257c70f09fde2f834e54c0895924da2d4dea92bf0fe4f8933f0"}, "dob": {"date": "1995-02-23T15:51:32.432Z", "age": 29}, "registered": {"date": "2011-05-30T09:40:08.957Z", "age": 13}, "phone": "037-85623112", "cell": "0959-847-5387", "id": {"name": "", "value": null}, "picture": {"large": "https://randomuser.me/api/portraits/women/88.jpg", "medium": "https://randomuser.me/api/portraits/med/women/88.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/women/88.jpg"}, "nat": "IR"}]', datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Alexis", "last": "Jones"}, "location": {"street": {"number": 9921, "name": "Pine Rd"}, "city": "Minto", "state": "Northwest Territories", "country": "Canada", "postcode": "O4J 4V9", "coordinates": {"latitude": "-65.9522", "longitude": "-32.4651"}, "timezone": {"offset": "-2:00", "description": "Mid-Atlantic"}}, "email": "alexis.jones@example.com", "login": {"uuid": "176fb965-209a-458f-b99a-2777f7ba0846", "username": "organicostrich245", "password": "mizuno", "salt": "kbKOvG9o", "md5": "0cb76f663841a2777078fefd0b9754cc", "sha1": "e5b87c1a2b32be8e2869923768edb6d3d65b46a9", "sha256": "30f3f12185d701279ff9ebf5fff19ffa147e0aad2d02cddf9f83a42402335812"}, "dob": {"date": "1969-12-06T05:47:42.611Z", "age": 54}, "registered": {"date": "2011-01-31T10:47:57.476Z", "age": 13}, "phone": "Z47 R05-2360", "cell": "N03 A19-7713", "id": {"name": "SIN", "value": "958620213"}, "picture": {"large": "https://randomuser.me/api/portraits/men/47.jpg", "medium": "https://randomuser.me/api/portraits/med/men/47.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/47.jpg"}, "nat": "CA"}]', datetime(2020, 3, 1, 0, 0, 30, 0, tzinfo=pytz.UTC).timestamp()),  # Same window
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Victor", "last": "Madsen"}, "location": {"street": {"number": 2216, "name": "Hirsev\\u00e6nget"}, "city": "Argerskov", "state": "Sj\\u00e6lland", "country": "Denmark", "postcode": 57512, "coordinates": {"latitude": "-40.8291", "longitude": "-79.1445"}, "timezone": {"offset": "-11:00", "description": "Midway Island, Samoa"}}, "email": "victor.madsen@example.com", "login": {"uuid": "f70341b8-3234-42ee-8e67-e4f4d9ed6645", "username": "happyladybug906", "password": "swords", "salt": "mjmsj55t", "md5": "b15043716cdd552cb2d6d77c9743e12b", "sha1": "d426b1bab46132c7a7a1843c9192d7492f3e945c", "sha256": "bf520866af0554544a0018b8734e6c3313cfcffc8451e4fc70019d47ba74ee4c"}, "dob": {"date": "1960-05-29T08:57:38.592Z", "age": 64}, "registered": {"date": "2021-08-23T16:15:27.776Z", "age": 3}, "phone": "85884323", "cell": "33653277", "id": {"name": "CPR", "value": "290560-1923"}, "picture": {"large": "https://randomuser.me/api/portraits/men/30.jpg", "medium": "https://randomuser.me/api/portraits/med/men/30.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/30.jpg"}, "nat": "DK"}]', datetime(2020, 3, 1, 0, 1, 0, 0, tzinfo=pytz.UTC).timestamp()),
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Luke", "last": "Simmons"}, "location": {"street": {"number": 5965, "name": "The Avenue"}, "city": "Mallow", "state": "South Dublin", "country": "Ireland", "postcode": 85090, "coordinates": {"latitude": "-3.0557", "longitude": "-135.0375"}, "timezone": {"offset": "-12:00", "description": "Eniwetok, Kwajalein"}}, "email": "luke.simmons@example.com", "login": {"uuid": "ad09d9dd-437a-4c91-8276-681b9cd01b49", "username": "lazykoala476", "password": "bruno1", "salt": "poDNXKed", "md5": "9832c3a6e64234d898fd18c2ecbe1078", "sha1": "eafde725d294d74258d9281549f1f36ea1cf045d", "sha256": "dfb4de5fc608fd3dfe5ccf4dee9174949309c64d998bc82ca34b2eba5a60a34d"}, "dob": {"date": "1975-08-28T10:55:56.148Z", "age": 49}, "registered": {"date": "2003-12-06T03:09:02.282Z", "age": 20}, "phone": "051-780-0725", "cell": "081-607-2880", "id": {"name": "PPS", "value": "6710341T"}, "picture": {"large": "https://randomuser.me/api/portraits/men/73.jpg", "medium": "https://randomuser.me/api/portraits/med/men/73.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"}, "nat": "IE"}]', datetime(2020, 3, 1, 0, 1, 30, 0, tzinfo=pytz.UTC).timestamp()),  # Same window
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Luke", "last": "Simmons"}, "location": {"street": {"number": 5965, "name": "The Avenue"}, "city": "Mallow", "state": "South Dublin", "country": "Ireland", "postcode": 85090, "coordinates": {"latitude": "-3.0557", "longitude": "-135.0375"}, "timezone": {"offset": "-12:00", "description": "Eniwetok, Kwajalein"}}, "email": "luke.simmons@example.com", "login": {"uuid": "ad09d9dd-437a-4c91-8276-681b9cd01b49", "username": "lazykoala476", "password": "bruno1", "salt": "poDNXKed", "md5": "9832c3a6e64234d898fd18c2ecbe1078", "sha1": "eafde725d294d74258d9281549f1f36ea1cf045d", "sha256": "dfb4de5fc608fd3dfe5ccf4dee9174949309c64d998bc82ca34b2eba5a60a34d"}, "dob": {"date": "1975-08-28T10:55:56.148Z", "age": 49}, "registered": {"date": "2003-12-06T03:09:02.282Z", "age": 20}, "phone": "051-780-0725", "cell": "081-607-2880", "id": {"name": "PPS", "value": "6710341T"}, "picture": {"large": "https://randomuser.me/api/portraits/men/73.jpg", "medium": "https://randomuser.me/api/portraits/med/men/73.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"}, "nat": "IE"}]', datetime(2020, 3, 1, 0, 2, 0, 0, tzinfo=pytz.UTC).timestamp()),
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Vincent", "last": "Patterson"}, "location": {"street": {"number": 7104, "name": "Prospect Rd"}, "city": "Miami", "state": "Nebraska", "country": "United States", "postcode": 60941, "coordinates": {"latitude": "86.1697", "longitude": "-0.7033"}, "timezone": {"offset": "-3:00", "description": "Brazil, Buenos Aires, Georgetown"}}, "email": "vincent.patterson@example.com", "login": {"uuid": "fa5ba889-38a9-4b9b-a215-4134bf16e385", "username": "beautifulfrog363", "password": "bigdog1", "salt": "pHOKTJT4", "md5": "4234719ab5344adbb961873cbf60ab86", "sha1": "f7febc9fda7443fb20c3a8531e9136b9f8faa409", "sha256": "de4efbb13b78f213d9c14f7c611d87ee7701a559d2be8497ca4276a52ade0769"}, "dob": {"date": "1948-05-26T23:21:58.048Z", "age": 76}, "registered": {"date": "2006-11-28T05:56:14.193Z", "age": 17}, "phone": "(790) 592-5303", "cell": "(716) 370-6027", "id": {"name": "SSN", "value": "359-34-9102"}, "picture": {"large": "https://randomuser.me/api/portraits/men/28.jpg", "medium": "https://randomuser.me/api/portraits/med/men/28.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/28.jpg"}, "nat": "US"}]', datetime(2020, 3, 1, 0, 2, 30, 0, tzinfo=pytz.UTC).timestamp()),  # Same window
        window.TimestampedValue(b'[{"gender": "female", "name": {"title": "Mrs", "first": "\\u0645\\u0644\\u06cc\\u0646\\u0627", "last": "\\u06a9\\u0627\\u0645\\u0631\\u0648\\u0627"}, "location": {"street": {"number": 4061, "name": "\\u067e\\u0627\\u0631\\u06a9 \\u0648\\u0644\\u06cc\\u0639\\u0635\\u0631"}, "city": "\\u0633\\u0627\\u0631\\u06cc", "state": "\\u0686\\u0647\\u0627\\u0631\\u0645\\u062d\\u0627\\u0644 \\u0648 \\u0628\\u062e\\u062a\\u06cc\\u0627\\u0631\\u06cc", "country": "Iran", "postcode": 73835, "coordinates": {"latitude": "20.2570", "longitude": "50.6576"}, "timezone": {"offset": "+7:00", "description": "Bangkok, Hanoi, Jakarta"}}, "email": "mlyn.khmrw@example.com", "login": {"uuid": "acad6e7c-be2e-4b49-8352-bf66ac6da114", "username": "blackpanda999", "password": "fishing", "salt": "4j5RpKcn", "md5": "d163f92c6ae8d46fa1d2240f5b1d57d0", "sha1": "880c6b3862230e91c5e432666094be69fdaa397d", "sha256": "fbc0a0b88b2cb5dc044010af844e0901d1f3d00e015b0c802d52a9eb3b7d9d50"}, "dob": {"date": "1968-12-03T13:45:52.670Z", "age": 55}, "registered": {"date": "2010-12-04T04:29:52.782Z", "age": 13}, "phone": "073-86003661", "cell": "0935-877-5319", "id": {"name": "", "value": null}, "picture": {"large": "https://randomuser.me/api/portraits/women/64.jpg", "medium": "https://randomuser.me/api/portraits/med/women/64.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/women/64.jpg"}, "nat": "IR"}]', datetime(2020, 3, 1, 0, 3, 0, 0, tzinfo=pytz.UTC).timestamp()),
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "Daniel", "last": "Pulido"}, "location": {"street": {"number": 1284, "name": "Circunvalaci\\u00f3n Guerrero"}, "city": "Charco Blanco", "state": "Queretaro", "country": "Mexico", "postcode": 18381, "coordinates": {"latitude": "24.6593", "longitude": "-35.2402"}, "timezone": {"offset": "-8:00", "description": "Pacific Time (US & Canada)"}}, "email": "daniel.pulido@example.com", "login": {"uuid": "3b2262a1-c2aa-4b40-a3f2-0265aeda11f2", "username": "yellowcat100", "password": "cartoon", "salt": "VlPQUqbO", "md5": "9eeba14ef5b284114193437e8eee7d22", "sha1": "7fca96976464ef4e41098df46f84fd440c662f48", "sha256": "b4a36d7833e9ef1232e83e732ff6b393a75a4881a16a1715d3d849ada5125aee"}, "dob": {"date": "1953-03-09T18:18:15.355Z", "age": 71}, "registered": {"date": "2010-06-29T09:13:07.965Z", "age": 14}, "phone": "(669) 861 1917", "cell": "(663) 176 6051", "id": {"name": "NSS", "value": "58 42 48 2166 5"}, "picture": {"large": "https://randomuser.me/api/portraits/men/33.jpg", "medium": "https://randomuser.me/api/portraits/med/men/33.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/33.jpg"}, "nat": "MX"}]', datetime(2020, 3, 1, 0, 3, 30, 0, tzinfo=pytz.UTC).timestamp()),  # Same window
        window.TimestampedValue(b'[{"gender": "male", "name": {"title": "Mr", "first": "\\u0627\\u0645\\u064a\\u0631\\u0639\\u0644\\u064a", "last": "\\u0646\\u0643\\u0648 \\u0646\\u0638\\u0631"}, "location": {"street": {"number": 6846, "name": "\\u067e\\u0627\\u0631\\u06a9 17 \\u0634\\u0647\\u0631\\u06cc\\u0648\\u0631"}, "city": "\\u0628\\u06cc\\u0631\\u062c\\u0646\\u062f", "state": "\\u06af\\u06cc\\u0644\\u0627\\u0646", "country": "Iran", "postcode": 67443, "coordinates": {"latitude": "15.0134", "longitude": "-102.2676"}, "timezone": {"offset": "-12:00", "description": "Eniwetok, Kwajalein"}}, "email": "myraaly.nkwnzr@example.com", "login": {"uuid": "2b0ae4fa-10e4-4901-ac03-3ab7b961daf0", "username": "angryduck314", "password": "roadking", "salt": "gc6qKcdO", "md5": "e432123e0d7664ffb7b9a855d1c5771f", "sha1": "25da165a46a8c465599ee4f894ce31a8fac090c8", "sha256": "cf52af0bbb23414c151596d751373e58874405293136dd1f9f88bc9b2ff7bf99"}, "dob": {"date": "1981-07-21T19:01:46.374Z", "age": 43}, "registered": {"date": "2016-03-18T08:11:27.803Z", "age": 8}, "phone": "075-40869038", "cell": "0923-705-8721", "id": {"name": "", "value": null}, "picture": {"large": "https://randomuser.me/api/portraits/men/47.jpg", "medium": "https://randomuser.me/api/portraits/med/men/47.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/47.jpg"}, "nat": "IR"}]', datetime(2020, 3, 1, 0, 4, 0, 0, tzinfo=pytz.UTC).timestamp()),
        window.TimestampedValue(b'[{"gender": "female", "name": {"title": "Ms", "first": "Orogosta", "last": "Yugay"}, "location": {"street": {"number": 3800, "name": "Laskava"}, "city": "Skvira", "state": "Ternopilska", "country": "Ukraine", "postcode": 47650, "coordinates": {"latitude": "76.9570", "longitude": "6.3790"}, "timezone": {"offset": "+10:00", "description": "Eastern Australia, Guam, Vladivostok"}}, "email": "orogosta.yugay@example.com", "login": {"uuid": "afce67cc-6da4-46ae-a6d0-0fb3942f3d83", "username": "beautifulbird993", "password": "surf", "salt": "2OqyO56A", "md5": "2add10e9c5d54070557842474fe6dcf4", "sha1": "3ad3d878313ba6cbbe0e762cf1cb1b8353a26447", "sha256": "72c6b805c6a5934ca330748506e1a47e2e9e7cbb0718460fbdeda1e1dabdf508"}, "dob": {"date": "1984-06-12T03:54:28.189Z", "age": 40}, "registered": {"date": "2006-02-11T19:06:54.619Z", "age": 18}, "phone": "(068) D92-8524", "cell": "(098) P47-2319", "id": {"name": "", "value": null}, "picture": {"large": "https://randomuser.me/api/portraits/women/60.jpg", "medium": "https://randomuser.me/api/portraits/med/women/60.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/women/60.jpg"}, "nat": "UA"}]', datetime(2020, 3, 1, 0, 4, 30, 0, tzinfo=pytz.UTC).timestamp())  # Same window
    ]

    (p
     | 'Create Events' >> beam.Create(events)
     | "decode">>beam.Map(decoder)
     | 'Apply Fixed Window' >> beam.WindowInto(window.FixedWindows(60))  
     | 'Add Window Start' >> beam.ParDo(AddWindowStartFn())
     | 'Group by Window' >> beam.GroupByKey()
     | 'Group By Window Start' >> beam.ParDo(Counter()) 
     | 'Print Results' >> beam.Map(print)
     )  
