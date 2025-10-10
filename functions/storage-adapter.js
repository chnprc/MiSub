/**
 * 数据存储适配器
 * 支持 KV 和 D1 两种存储方式
 * 提供统一的数据存储访问接口
 */

// 存储类型常量
export const STORAGE_TYPES = {
    KV: 'kv',
    D1: 'd1'
};

// 数据键映射
const DATA_KEYS = {
    SUBSCRIPTIONS: 'misub_subscriptions_v1',
    PROFILES: 'misub_profiles_v1',
    SETTINGS: 'worker_settings_v1'
};

/**
 * KV 存储适配器
 */
class KVStorageAdapter {
    constructor(kvNamespace) {
        this.kv = kvNamespace;
    }

    async get(key, type = 'json') {
        try {
            return await this.kv.get(key, type);
        } catch (error) {
            console.error(`[KV] Failed to get key ${key}:`, error);
            return null;
        }
    }

    async put(key, value) {
        try {
            const data = typeof value === 'string' ? value : JSON.stringify(value);
            await this.kv.put(key, data);
            return true;
        } catch (error) {
            console.error(`[KV] Failed to put key ${key}:`, error);
            throw error;
        }
    }

    async delete(key) {
        try {
            await this.kv.delete(key);
            return true;
        } catch (error) {
            console.error(`[KV] Failed to delete key ${key}:`, error);
            throw error;
        }
    }

    async list(prefix = '') {
        try {
            const result = await this.kv.list({ prefix });
            return result.keys.map(key => key.name);
        } catch (error) {
            console.error(`[KV] Failed to list keys with prefix ${prefix}:`, error);
            return [];
        }
    }

    getType() {
        return STORAGE_TYPES.KV;
    }
}

/**
 * D1 存储适配器
 */
class D1StorageAdapter {
    constructor(d1Database) {
        this.db = d1Database;
    }

    async get(key, type = 'json') {
        try {
            const result = await this.db.prepare('SELECT value FROM settings WHERE key = ?').bind(key).first();
            if (!result) return null;
            
            return type === 'json' ? JSON.parse(result.value) : result.value;
        } catch (error) {
            console.error(`[D1] Failed to get key ${key}:`, error);
            return null;
        }
    }

    async put(key, value) {
        try {
            const data = typeof value === 'string' ? value : JSON.stringify(value);
            await this.db.prepare(`
                INSERT OR REPLACE INTO settings (key, value, updated_at) 
                VALUES (?, ?, datetime('now'))
            `).bind(key, data).run();
            return true;
        } catch (error) {
            console.error(`[D1] Failed to put key ${key}:`, error);
            throw error;
        }
    }

    async delete(key) {
        try {
            await this.db.prepare('DELETE FROM settings WHERE key = ?').bind(key).run();
            return true;
        } catch (error) {
            console.error(`[D1] Failed to delete key ${key}:`, error);
            throw error;
        }
    }

    async list(prefix = '') {
        try {
            const result = await this.db.prepare('SELECT key FROM settings WHERE key LIKE ?').bind(`${prefix}%`).all();
            return result.results.map(row => row.key);
        } catch (error) {
            console.error(`[D1] Failed to list keys with prefix ${prefix}:`, error);
            return [];
        }
    }

    getType() {
        return STORAGE_TYPES.D1;
    }

    // D1 特有方法：初始化数据库表
    async initTables() {
        try {
            // 创建 subscriptions 表
            await this.db.prepare(`
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `).run();

            // 创建 profiles 表
            await this.db.prepare(`
                CREATE TABLE IF NOT EXISTS profiles (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `).run();

            // 创建 settings 表
            await this.db.prepare(`
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `).run();

            // 创建索引
            await this.db.prepare('CREATE INDEX IF NOT EXISTS idx_subscriptions_updated_at ON subscriptions(updated_at)').run();
            await this.db.prepare('CREATE INDEX IF NOT EXISTS idx_profiles_updated_at ON profiles(updated_at)').run();
            await this.db.prepare('CREATE INDEX IF NOT EXISTS idx_settings_updated_at ON settings(updated_at)').run();

            console.log('[D1] Database tables initialized successfully');
            return true;
        } catch (error) {
            console.error('[D1] Failed to initialize tables:', error);
            throw error;
        }
    }
}

/**
 * 存储工厂类
 */
export class StorageFactory {
    static create(type, resource) {
        switch (type) {
            case STORAGE_TYPES.KV:
                if (!resource) {
                    throw new Error('KV namespace is required for KV storage');
                }
                return new KVStorageAdapter(resource);
            
            case STORAGE_TYPES.D1:
                if (!resource) {
                    throw new Error('D1 database is required for D1 storage');
                }
                const adapter = new D1StorageAdapter(resource);
                // 自动初始化表结构
                adapter.initTables().catch(error => {
                    console.warn('[D1] Failed to auto-initialize tables:', error);
                });
                return adapter;
            
            default:
                throw new Error(`Unsupported storage type: ${type}`);
        }
    }
}

/**
 * 数据迁移器
 */
export class DataMigrator {
    constructor(env) {
        this.env = env;
    }

    async migrate(fromType, toType) {
        console.log(`Starting migration from ${fromType} to ${toType}`);
        
        try {
            const fromStorage = StorageFactory.create(fromType, this.getResource(fromType));
            const toStorage = StorageFactory.create(toType, this.getResource(toType));
            
            const migrationResults = {
                subscriptions: false,
                profiles: false,
                settings: false,
                errors: []
            };

            // 迁移订阅数据
            try {
                const subscriptions = await fromStorage.get(DATA_KEYS.SUBSCRIPTIONS);
                if (subscriptions) {
                    await toStorage.put(DATA_KEYS.SUBSCRIPTIONS, subscriptions);
                    migrationResults.subscriptions = true;
                    console.log('[Migration] Subscriptions migrated successfully');
                }
            } catch (error) {
                migrationResults.errors.push(`Subscriptions: ${error.message}`);
                console.error('[Migration] Failed to migrate subscriptions:', error);
            }

            // 迁移配置文件数据
            try {
                const profiles = await fromStorage.get(DATA_KEYS.PROFILES);
                if (profiles) {
                    await toStorage.put(DATA_KEYS.PROFILES, profiles);
                    migrationResults.profiles = true;
                    console.log('[Migration] Profiles migrated successfully');
                }
            } catch (error) {
                migrationResults.errors.push(`Profiles: ${error.message}`);
                console.error('[Migration] Failed to migrate profiles:', error);
            }

            // 迁移设置数据
            try {
                const settings = await fromStorage.get(DATA_KEYS.SETTINGS);
                if (settings) {
                    await toStorage.put(DATA_KEYS.SETTINGS, settings);
                    migrationResults.settings = true;
                    console.log('[Migration] Settings migrated successfully');
                }
            } catch (error) {
                migrationResults.errors.push(`Settings: ${error.message}`);
                console.error('[Migration] Failed to migrate settings:', error);
            }

            console.log('[Migration] Migration completed:', migrationResults);
            return migrationResults;

        } catch (error) {
            console.error('[Migration] Migration failed:', error);
            throw error;
        }
    }

    getResource(type) {
        switch (type) {
            case STORAGE_TYPES.KV:
                return this.env.MISUB_KV;
            case STORAGE_TYPES.D1:
                return this.env.MISUB_DB;
            default:
                throw new Error(`Unknown storage type: ${type}`);
        }
    }
}

/**
 * 存储健康检查器
 */
export class StorageHealthChecker {
    static async check(storage) {
        const health = {
            type: storage.getType(),
            available: false,
            readable: false,
            writable: false,
            error: null
        };

        try {
            // 测试读取
            const testKey = 'health_check_test';
            const testValue = { timestamp: Date.now(), test: true };
            
            // 测试写入
            await storage.put(testKey, testValue);
            health.writable = true;
            
            // 测试读取
            const retrieved = await storage.get(testKey);
            health.readable = retrieved !== null;
            
            // 清理测试数据
            await storage.delete(testKey);
            
            health.available = health.readable && health.writable;
            
        } catch (error) {
            health.error = error.message;
            console.error(`[HealthCheck] Storage health check failed for ${health.type}:`, error);
        }

        return health;
    }
}

// 导出数据键常量供外部使用
export { DATA_KEYS };
