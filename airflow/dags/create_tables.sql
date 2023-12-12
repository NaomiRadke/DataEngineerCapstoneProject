-- DATA MODEL TABLES
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

CREATE TABLE public.immigration (
    cicid int4,
    i94res int4,
    i94mode int4,
    i94addr varchar,
    i94cit int4,
    i94bir int4,
    i94visa int4,
    arrdate varchar,
    depdate varchar,
    biryear int4,
    dtaddto varchar,
    gender varchar,
    airline varchar,
    admnum int4,
    fltno int4,
    visatype varchar,
    durationStay int4,
    i94yr int4,
    i94mon int4,
    CONSTRAINT immigration_pkey PRIMARY KEY (cicid)
);

CREATE TABLE public.arrivalDate (
    arrivalDate varchar,
    "day" int4,
    "month" int4,
    "year" int4,
    dayOfWeek int4,
    weekOfYear int4,
    CONSTRAINT arrivaldate_pkey PRIMARY KEY (arrivalDate)
);

CREATE TABLE public.demographics (
    state varchar,
    stateCode int4,
    malePopulation int8,
    femalePopulation int8,
    totalPopulation int8,
    numberOfVeterans int8,
    foreignBorn int8,
    medianAge float,
    averageHouseholdSize float,
    blackOrAfricanAmerican int8,
    americanIndianAndAlaskaNative int8,
    hispanicOrLatino int8,
    asian int8,
    white int8,
    CONSTRAINT demographics_pkey PRIMARY KEY (stateCode)
);   


CREATE TABLE public.countries (
    countryCode int4,
    countryName varchar,
    CONSTRAINT countries_pkey PRIMARY KEY (countryCode)
);



