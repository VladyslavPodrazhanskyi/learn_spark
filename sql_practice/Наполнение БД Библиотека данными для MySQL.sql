INSERT INTO `books` (`b_id`, `b_name`, `b_year`, `b_quantity`)
VALUES
(1, 'Eugene Onegin', 1985, 2),
(2, 'The Fishermen and the Golden Fish', 1990, 3),
(3, 'Foundation and Empire', 2000, 5),
(4, 'Programming Psychology', 1998, 1),
(5, 'The C++ Programming Language', 1996, 3),
(6, 'Course of Theoretical Physics', 1981, 12),
(7, 'The Art of Computer Programming', 1993, 7);

INSERT INTO `authors` (`a_id`, `a_name`)
VALUES
(1, 'Donald Knuth'),
(2, 'Isaac Asimov'),
(3, 'Dale Carnegie'),
(4, 'Lev Landau'),
(5, 'Evgeny Lifshitz'),
(6, 'Bjarne Stroustrup'),
(7, 'Alexander Pushkin');


INSERT INTO `genres` (`g_id`, `g_name`)
VALUES
(1, 'Poetry'),
(2, 'Programming'),
(3, 'Psychology'),
(4, 'Science'),
(5, 'Classic'),
(6, 'Science Fiction');


INSERT INTO `subscribers` (`s_id`, `s_name`)
VALUES
(1, 'Ivanov I.I.'),
(2, 'Petrov P.P.'),
(3, 'Sidorov S.S.'),
(4, 'Sidorov S.S.');

INSERT INTO `m2m_books_authors`
(`b_id`, `a_id`)
VALUES
(1, 7),
(2, 7),
(3, 2),
(4, 3),
(4, 6),
(5, 6),
(6, 5),
(6, 4),
(7, 1);


INSERT INTO `m2m_books_genres` (`b_id`, `g_id`)
VALUES
(1, 1),
(1, 5),
(2, 1),
(2, 5),
(3, 6),
(4, 2),
(4, 3),
(5, 2),
(6, 5),
(7, 2),
(7, 5);


INSERT INTO `subscriptions` (`sb_id`, `sb_subscriber`, `sb_book`, `sb_start`, `sb_finish`, `sb_is_active`)
VALUES
(100, 1, 3, '2011-01-12', '2011-02-12', 'N'),
(2, 1, 1, '2011-01-12', '2011-02-12', 'N'),
(3, 3, 3, '2012-05-17', '2012-07-17', 'Y'),
(42, 1, 2, '2012-06-11', '2012-08-11', 'N'),
(57, 4, 5, '2012-06-11', '2012-08-11', 'N'),
(61, 1, 7, '2014-08-03', '2014-10-03', 'N'),
(62, 3, 5, '2014-08-03', '2014-10-03', 'Y'),
(86, 3, 1, '2014-08-03', '2014-09-03', 'Y'),
(91, 4, 1, '2015-10-07', '2015-03-07', 'Y'),
(95, 1, 4, '2015-10-07', '2015-11-07', 'N'),
(99, 4, 4, '2015-10-08', '2025-11-08', 'Y');