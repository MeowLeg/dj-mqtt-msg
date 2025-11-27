create table if not exists pic (
    id integer primary key autoincrement,
    path text not null default '',
    stream_url text not null default '',
    uuid text not null default '',
    project_uuid text not null default '',
    organization_uuid text not null default '',
    stream_md5 text not null default '',
    predicted integer not null default 0,
    pts integer not null default 0,
    create_date text default (strftime('%Y-%m-%d %H:%M:%S', datetime('now', 'localtime')))
);

create table if not exists sn (
    id integer primary key autoincrement,
    sn text unique,
    uuid text unique,
    project_uuid text not null default '',
    organization_uuid text not null default '',
    created_timestamp integer not null default (strftime('%s', datetime('now', 'localtime'))),
    create_date text default (strftime('%Y-%m-%d %H:%M:%S', datetime('now', 'localtime')))
);

create table if not exists predict (
    id integer primary key autoincrement,
    path text not null default '',
    uuid text not null default '',
    tag text not null default '',
    predict_path text not null default '',
    stream_url text not null default '',
    project_uuid text not null default '',
    organization_uuid text not null default '',
    stream_md5 text not null default '',
    create_date text default (strftime('%Y-%m-%d %H:%M:%S', timestamp('now', 'localtime')))
);

-- 不再使用了
-- create table if not exists stream (
--     id integer primary key autoincrement,
--     uuid text not null default '',
--     stream_url text not null default '',
--     project_uuid text not null default '',
--     organization_uuid text not null default '',
--     sn text not null default '',
--     is_over integer not null default 0,
--     created_timestamp integer not null default (strftime('%s', datetime('now', 'localtime'))),
--     create_date text default (strftime('%Y-%m-%d %H:%M:%S', datetime('now', 'localtime')))
-- );

-- 每个流(sn/uuid)都有对应的相关信息，包括uuid和一个数组，该数组里有需要检测的场景名称与code
-- 由于这个数组的存在，必须另建立一张表，否则需要在stream表中加个json字段
create table if not exists stream_tag (
    id integer primary key autoincrement,
    uuid text not null default '', -- sn表的uuid
    code text not null default '',
    name text not null default ''
);
