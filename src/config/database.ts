import { createPool, Pool } from 'mysql2/promise';

export function createDatabasePool(): Pool {
	return createPool({
		host: process.env.MYSQL_HOST ?? 'localhost',
		port: Number(process.env.MYSQL_PORT ?? 3306),
		user: process.env.MYSQL_USER ?? 'root',
		password: process.env.MYSQL_PASSWORD ?? '',
		database: process.env.MYSQL_DATABASE ?? 'hotel',
		waitForConnections: true,
		connectionLimit: Number(process.env.MYSQL_CONNECTION_LIMIT ?? 5),
		dateStrings: false
	});
}
