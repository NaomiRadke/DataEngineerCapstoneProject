-- DATA MODEL TABLES

DROP TABLE IF EXISTS public.immigration;
CREATE TABLE IF NOT EXISTS public.immigration (
    cicid int4,
    i94yr int4,
    i94mon int4,
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
    CONSTRAINT immigration_pkey PRIMARY KEY (cicid)
);

DROP TABLE IF EXISTS public.arrivalDate;
CREATE TABLE IF NOT EXISTS public.arrivalDate (
    arrivalDate varchar,
    "day" int4,
    "month" int4,
    "year" int4,
    dayOfWeek int4,
    weekOfYear int4,
    CONSTRAINT arrivaldate_pkey PRIMARY KEY (arrivalDate)
);

DROP TABLE IF EXISTS public.demographics;
CREATE TABLE IF NOT EXISTS public.demographics (
    stateCode int4,
    state varchar,
    medianAge float,
    malePopulation int4,
    femalePopulation int4,
    totalPopulation int4,
    numberOfVeterans int4,
    foreignBorn int4,
    averageHouseholdSize float,
    blackOrAfricanAmerican int4,
    hispanicOrLatino int4,
    americanIndianAndAlaskaNative int4,
    asian int4,
    white int4,
    CONSTRAINT demographics_pkey PRIMARY KEY (stateCode)
);   

DROP TABLE IF EXISTS public.countries;
CREATE TABLE IF NOT EXISTS public.countries (
    countryCode int4,
    countryName varchar,
    CONSTRAINT countries_pkey PRIMARY KEY (countryCode)
);



