package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"log/slog"
	mrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

// ============================================================
// CONFIG
// ============================================================

type Config struct {
	MainBotToken  string
	BotTokens     []string
	StringSession string
	APIID         int
	APIHash       string
	AdminID       int64
	DBURI         string
	RedisURI      string
	DBChannelID   int64
	LogChannelID  int64
	MainChannelID int64
	FQDN          string
	Port          string
	DashboardToken string

	StreamConcurrency int
	StreamBufferCount int
	StreamTimeoutSec  int
	StreamMaxRetries  int

	PasswordPromptVideoURL   string
	PasswordPromptImages     []string
	ContactTelegramUsername  string
	ContactInstagramUsername string
	SplashAboutText          string
}

func loadConfig() (*Config, error) {
	cfg := &Config{}
	var errs []string

	mainToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if mainToken == "" {
		errs = append(errs, "BOT_TOKEN missing")
	} else {
		cfg.MainBotToken = mainToken
		cfg.BotTokens = append(cfg.BotTokens, mainToken)
	}

	for i := 1; i <= 20; i++ {
		t := strings.TrimSpace(os.Getenv(fmt.Sprintf("MULTI_TOKEN%d", i)))
		if t != "" {
			cfg.BotTokens = append(cfg.BotTokens, t)
		}
	}

	cfg.StringSession = strings.TrimSpace(os.Getenv("STRING_SESSION"))

	apiIDStr := os.Getenv("API_ID")
	if apiIDStr == "" {
		errs = append(errs, "API_ID missing")
	} else if id, err := strconv.Atoi(apiIDStr); err != nil {
		errs = append(errs, "API_ID invalid")
	} else {
		cfg.APIID = id
	}

	cfg.APIHash = os.Getenv("API_HASH")
	if cfg.APIHash == "" {
		errs = append(errs, "API_HASH missing")
	}

	adminStr := os.Getenv("ADMIN_ID")
	if adminStr == "" {
		errs = append(errs, "ADMIN_ID missing")
	} else if id, err := strconv.ParseInt(adminStr, 10, 64); err != nil {
		errs = append(errs, "ADMIN_ID invalid")
	} else {
		cfg.AdminID = id
	}

	cfg.DBURI = firstEnv("DB_URI", "DATABASE_URL", "MONGODB_URI")
	if cfg.DBURI == "" {
		errs = append(errs, "DB_URI, DATABASE_URL or MONGODB_URI missing")
	}

	cfg.RedisURI = firstEnv("REDIS_URI", "REDIS_URL")
	if cfg.RedisURI == "" {
		errs = append(errs, "REDIS_URI or REDIS_URL missing")
	}

	cfg.FQDN = os.Getenv("FQDN")
	if cfg.FQDN == "" {
		errs = append(errs, "FQDN missing")
	}

	dbChStr := os.Getenv("DB_CHANNEL_ID")
	if dbChStr == "" {
		errs = append(errs, "DB_CHANNEL_ID missing")
	} else if id, err := strconv.ParseInt(dbChStr, 10, 64); err != nil {
		errs = append(errs, "DB_CHANNEL_ID invalid")
	} else {
		cfg.DBChannelID = id
	}

	logChStr := os.Getenv("LOG_CHANNEL_ID")
	if logChStr == "" {
		errs = append(errs, "LOG_CHANNEL_ID missing")
	} else if id, err := strconv.ParseInt(logChStr, 10, 64); err != nil {
		errs = append(errs, "LOG_CHANNEL_ID invalid")
	} else {
		cfg.LogChannelID = id
	}

	if v := os.Getenv("MAIN_CHANNEL_ID"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.MainChannelID = id
		}
	}
	cfg.Port = firstEnv("PORT", "8080")

	cfg.DashboardToken = strings.TrimSpace(os.Getenv("ADMIN_DASHBOARD_TOKEN"))
	if cfg.DashboardToken == "" {
		cfg.DashboardToken = randomToken(24)
	}

	cfg.StreamConcurrency = envInt("STREAM_CONCURRENCY", 4)
	cfg.StreamBufferCount = envInt("STREAM_BUFFER_COUNT", 8)
	cfg.StreamTimeoutSec  = envInt("STREAM_TIMEOUT_SEC", 30)
	cfg.StreamMaxRetries  = envInt("STREAM_MAX_RETRIES", 3)

	cfg.PasswordPromptVideoURL = strings.TrimSpace(os.Getenv("PASSWORD_PROMPT_VIDEO_URL"))

	if raw := strings.TrimSpace(os.Getenv("PASSWORD_PROMPT_IMAGES")); raw != "" {
		for _, u := range strings.Split(raw, ",") {
			if u = strings.TrimSpace(u); u != "" {
				cfg.PasswordPromptImages = append(cfg.PasswordPromptImages, u)
			}
		}
	}

	cfg.ContactTelegramUsername = strings.TrimPrefix(strings.TrimSpace(os.Getenv("CONTACT_TELEGRAM_USERNAME")), "@")
	if cfg.ContactTelegramUsername == "" {
		cfg.ContactTelegramUsername = "raj_dev_01"
	}
	cfg.ContactInstagramUsername = strings.TrimPrefix(strings.TrimSpace(os.Getenv("CONTACT_INSTAGRAM_USERNAME")), "@")

	cfg.SplashAboutText = strings.TrimSpace(os.Getenv("SPLASH_ABOUT_TEXT"))
	if cfg.SplashAboutText == "" {
		cfg.SplashAboutText = "Empowering Developers Through Authentic Learning  •  " +
			"Welcome to this platform — your dedicated destination for technical growth and mastery. " +
			"Our mission is to provide a clean, focused, and high-quality educational environment designed " +
			"specifically for aspiring developers and tech enthusiasts."
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("config errors: %s", strings.Join(errs, " | "))
	}
	return cfg, nil
}

func (c *Config) baseURL() string {
	fqdn := strings.TrimRight(c.FQDN, "/")
	if !strings.HasPrefix(fqdn, "http") {
		fqdn = "https://" + fqdn
	}
	return fqdn
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func envInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultVal
}

// ============================================================
// DATABASE (MONGODB)
// ============================================================

type FileRecord struct {
	ID            string     `bson:"_id" json:"id"`
	MessageID     int        `bson:"message_id" json:"message_id"`
	ChannelID     int64      `bson:"channel_id" json:"channel_id"`
	FileName      string     `bson:"file_name" json:"file_name"`
	FileSize      int64      `bson:"file_size" json:"file_size"`
	MimeType      string     `bson:"mime_type" json:"mime_type"`
	Hash          string     `bson:"hash" json:"hash"`
	UploaderID    int64      `bson:"uploader_id" json:"uploader_id"`
	UploaderName  string     `bson:"uploader_name" json:"uploader_name"`
	CreatedAt     time.Time  `bson:"created_at" json:"created_at"`
	ExpiresAt     *time.Time `bson:"expires_at,omitempty" json:"expires_at,omitempty"`
	ViewCount     int64      `bson:"view_count" json:"view_count"`
	PasswordHash  *string    `bson:"password_hash,omitempty" json:"password_hash,omitempty"`
	PasswordPlain *string    `bson:"password_plain,omitempty" json:"password_plain,omitempty"`
	Subject       string     `bson:"subject" json:"subject"`
	Chapter       string     `bson:"chapter" json:"chapter"`
	Year          int        `bson:"year" json:"year"`
	EpisodeLabel  string     `bson:"episode_label" json:"episode_label"`
	GroupID       *string    `bson:"group_id,omitempty" json:"group_id,omitempty"`
	QualityLabel  string     `bson:"quality_label,omitempty" json:"quality_label,omitempty"`
	QualityRank   int        `bson:"quality_rank,omitempty" json:"quality_rank,omitempty"`
}

type UserRecord struct {
	ID        int64     `bson:"_id" json:"id"`
	Username  string    `bson:"username" json:"username"`
	FirstName string    `bson:"first_name" json:"first_name"`
	IsBanned  bool      `bson:"is_banned" json:"is_banned"`
	JoinedAt  time.Time `bson:"joined_at" json:"joined_at"`
}

type ApprovalRecord struct {
	AccessID       int        `bson:"access_id" json:"access_id"`
	DeviceID       string     `bson:"device_id" json:"device_id"`
	Slug           string     `bson:"slug" json:"slug"`
	VisitorName    string     `bson:"visitor_name" json:"visitor_name"`
	Approved       bool       `bson:"approved" json:"approved"`
	Blocked        bool       `bson:"blocked" json:"blocked"`
	CreatedAt      time.Time  `bson:"created_at" json:"created_at"`
	ApprovedAt     *time.Time `bson:"approved_at,omitempty" json:"approved_at,omitempty"`
	LastNotifiedAt *time.Time `bson:"last_notified_at,omitempty" json:"last_notified_at,omitempty"`
}

type VisitorProfile struct {
	DeviceID  string    `bson:"_id" json:"device_id"`
	Name      string    `bson:"name" json:"name"`
	About     string    `bson:"about" json:"about"`
	Email     string    `bson:"email" json:"email"`
	Phone     string    `bson:"phone" json:"phone"`
	Instagram string    `bson:"instagram" json:"instagram"`
	Facebook  string    `bson:"facebook" json:"facebook"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

type DB struct {
	client          *mongo.Client
	db              *mongo.Database
	files           *mongo.Collection
	users           *mongo.Collection
	approvals       *mongo.Collection
	visitorProfiles *mongo.Collection
	fileViews       *mongo.Collection
}

func newDB(ctx context.Context, dsn string) (*DB, error) {
	clientOpts := options.Client().
		ApplyURI(dsn).
		SetMaxPoolSize(50).
		SetMinPoolSize(5).
		SetMaxConnIdleTime(5 * time.Minute)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("ping mongo: %w", err)
	}

	dbName := "astratoonix"
	if envDB := os.Getenv("DB_NAME"); envDB != "" {
		dbName = envDB
	} else if u, err := url.Parse(dsn); err == nil && len(strings.TrimPrefix(u.Path, "/")) > 0 {
		path := strings.TrimPrefix(u.Path, "/")
		if idx := strings.Index(path, "?"); idx != -1 {
			path = path[:idx]
		}
		if path != "" {
			dbName = path
		}
	}

	database := client.Database(dbName)
	db := &DB{
		client:          client,
		db:              database,
		files:           database.Collection("files"),
		users:           database.Collection("users"),
		approvals:       database.Collection("approvals"),
		visitorProfiles: database.Collection("visitor_profiles"),
		fileViews:       database.Collection("file_views"),
	}

	if err := db.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("ensure indexes: %w", err)
	}

	return db, nil
}

func (db *DB) ensureIndexes(ctx context.Context) error {
	indexCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	_, err := db.files.Indexes().CreateMany(indexCtx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "message_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "created_at", Value: -1}}},
		{Keys: bson.D{{Key: "view_count", Value: -1}}},
		{Keys: bson.D{{Key: "subject", Value: 1}}},
		{Keys: bson.D{{Key: "year", Value: -1}}},
	})
	if err != nil {
		return fmt.Errorf("files indexes: %w", err)
	}

	_, err = db.approvals.Indexes().CreateMany(indexCtx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "device_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "access_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "created_at", Value: -1}}},
	})
	if err != nil {
		return fmt.Errorf("approvals indexes: %w", err)
	}

	_, err = db.fileViews.Indexes().CreateOne(indexCtx, mongo.IndexModel{
		Keys:    bson.D{{Key: "file_id", Value: 1}, {Key: "device_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func (db *DB) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.client.Disconnect(ctx)
}

func (db *DB) saveFile(ctx context.Context, f *FileRecord) error {
	if f.CreatedAt.IsZero() {
		f.CreatedAt = time.Now().UTC()
	}
	filter := bson.M{"message_id": f.MessageID}
	update := bson.M{
		"$set": bson.M{
			"_id":            f.ID,
			"message_id":     f.MessageID,
			"channel_id":     f.ChannelID,
			"file_name":      f.FileName,
			"file_size":      f.FileSize,
			"mime_type":      f.MimeType,
			"hash":           f.Hash,
			"uploader_id":    f.UploaderID,
			"uploader_name":  f.UploaderName,
			"created_at":     f.CreatedAt,
			"group_id":       f.GroupID,
			"quality_label":  f.QualityLabel,
			"quality_rank":   f.QualityRank,
		},
	}
	_, err := db.files.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

func (db *DB) getFileByID(ctx context.Context, id string) (*FileRecord, error) {
	var f FileRecord
	err := db.files.FindOne(ctx, bson.M{"_id": id}).Decode(&f)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (db *DB) setPassword(ctx context.Context, id string, hash *string, plain *string) error {
	update := bson.M{"$set": bson.M{"password_hash": hash, "password_plain": plain}}
	res, err := db.files.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("file not found: %s", id)
	}
	return nil
}

func (db *DB) recordUniqueView(ctx context.Context, fileID, deviceID string) (bool, int64, error) {
	viewDoc := bson.M{
		"_id":             fmt.Sprintf("%s:%s", fileID, deviceID),
		"file_id":         fileID,
		"device_id":       deviceID,
		"first_viewed_at": time.Now().UTC(),
	}
	_, err := db.fileViews.InsertOne(ctx, viewDoc)
	isNew := false
	if err == nil {
		isNew = true
	} else if !mongo.IsDuplicateKeyError(err) {
		return false, 0, err
	}

	if isNew {
		opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
		var updated FileRecord
		err = db.files.FindOneAndUpdate(
			ctx,
			bson.M{"_id": fileID},
			bson.M{"$inc": bson.M{"view_count": 1}},
			opts,
		).Decode(&updated)
		if err != nil {
			return true, 0, err
		}
		return true, updated.ViewCount, nil
	}

	var f FileRecord
	err = db.files.FindOne(ctx, bson.M{"_id": fileID}).Decode(&f)
	if err != nil {
		return false, 0, err
	}
	return false, f.ViewCount, nil
}

func (db *DB) searchFiles(ctx context.Context, query string, limit int) ([]*FileRecord, error) {
	filter := bson.M{"file_name": primitive.Regex{Pattern: query, Options: "i"}}
	opts := options.Find().SetSort(bson.D{{Key: "view_count", Value: -1}}).SetLimit(int64(limit))
	cursor, err := db.files.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) topFilesByViews(ctx context.Context, limit int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "view_count", Value: -1}}).SetLimit(int64(limit))
	cursor, err := db.files.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) newestFiles(ctx context.Context, limit int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit))
	cursor, err := db.files.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) setFileTag(ctx context.Context, fileID, subject, chapter string) (bool, error) {
	res, err := db.files.UpdateOne(ctx, bson.M{"_id": fileID}, bson.M{"$set": bson.M{"subject": subject, "chapter": chapter}})
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) listSubjects(ctx context.Context) ([]string, error) {
	values, err := db.files.Distinct(ctx, "subject", bson.M{"subject": bson.M{"$ne": "", "$exists": true}})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, v := range values {
		if s, ok := v.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (db *DB) listFilesBySubject(ctx context.Context, subject string) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "chapter", Value: 1}, {Key: "created_at", Value: 1}})
	cursor, err := db.files.Find(ctx, bson.M{"subject": subject}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

// listGroupFiles returns all quality-variant files belonging to a multi-quality
// upload group, sorted highest quality first.
func (db *DB) listGroupFiles(ctx context.Context, groupID string) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "quality_rank", Value: -1}, {Key: "created_at", Value: 1}})
	cursor, err := db.files.Find(ctx, bson.M{"group_id": groupID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) setFileYear(ctx context.Context, fileID string, year int) (bool, error) {
	res, err := db.files.UpdateOne(ctx, bson.M{"_id": fileID}, bson.M{"$set": bson.M{"year": year}})
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) setFileEpisode(ctx context.Context, fileID, label string) (bool, error) {
	res, err := db.files.UpdateOne(ctx, bson.M{"_id": fileID}, bson.M{"$set": bson.M{"episode_label": label}})
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) listYears(ctx context.Context) ([]int, error) {
	values, err := db.files.Distinct(ctx, "year", bson.M{"year": bson.M{"$ne": 0, "$exists": true}})
	if err != nil {
		return nil, err
	}
	var out []int
	for _, v := range values {
		switch n := v.(type) {
		case int32:
			out = append(out, int(n))
		case int64:
			out = append(out, int(n))
		case int:
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] > out[j] })
	return out, nil
}

func (db *DB) listFilesByYear(ctx context.Context, year int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "episode_label", Value: 1}, {Key: "created_at", Value: 1}})
	cursor, err := db.files.Find(ctx, bson.M{"year": year}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) sumViews(ctx context.Context) (int64, error) {
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$view_count"}}},
		}}},
	}
	cursor, err := db.files.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var res []struct {
		Total int64 `bson:"total"`
	}
	if err := cursor.All(ctx, &res); err != nil || len(res) == 0 {
		return 0, err
	}
	return res[0].Total, nil
}

func (db *DB) setExpiry(ctx context.Context, id string, expiresAt *time.Time) error {
	res, err := db.files.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"expires_at": expiresAt}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("file not found: %s", id)
	}
	return nil
}

func (db *DB) incrementViews(ctx context.Context, id string) (int64, error) {
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updated FileRecord
	err := db.files.FindOneAndUpdate(ctx, bson.M{"_id": id}, bson.M{"$inc": bson.M{"view_count": 1}}, opts).Decode(&updated)
	return updated.ViewCount, err
}

func (db *DB) deleteFileByMsgID(ctx context.Context, msgID int) (bool, error) {
	res, err := db.files.DeleteOne(ctx, bson.M{"message_id": msgID})
	if err != nil {
		return false, err
	}
	return res.DeletedCount > 0, nil
}

// deleteFileByID deletes a file by its own _id (the short link/slug shown to
// users, e.g. via /delete <file_id>). This is safer than deleteFileByMsgID
// because message_id is not guaranteed unique (multi-quality upload groups
// can share a message_id across variants), which previously could cause a
// delete to silently match 0 documents while the bot still reported success.
func (db *DB) deleteFileByID(ctx context.Context, id string) (bool, error) {
	res, err := db.files.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return false, err
	}
	if res.DeletedCount > 0 {
		_, _ = db.fileViews.DeleteMany(ctx, bson.M{"file_id": id})
		return true, nil
	}
	return false, nil
}

func (db *DB) countFiles(ctx context.Context) (int64, error) {
	return db.files.CountDocuments(ctx, bson.M{})
}

func (db *DB) deleteAllFiles(ctx context.Context) (int64, error) {
	res, err := db.files.DeleteMany(ctx, bson.M{})
	if err != nil {
		return 0, err
	}
	_, _ = db.fileViews.DeleteMany(ctx, bson.M{})
	return res.DeletedCount, nil
}

func (db *DB) upsertUser(ctx context.Context, u *UserRecord) error {
	filter := bson.M{"_id": u.ID}
	update := bson.M{
		"$set": bson.M{"username": u.Username, "first_name": u.FirstName},
		"$setOnInsert": bson.M{
			"_id":       u.ID,
			"is_banned": false,
			"joined_at": time.Now().UTC(),
		},
	}
	_, err := db.users.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

func (db *DB) getUser(ctx context.Context, id int64) (*UserRecord, error) {
	var u UserRecord
	err := db.users.FindOne(ctx, bson.M{"_id": id}).Decode(&u)
	return &u, err
}

func (db *DB) banUser(ctx context.Context, id int64, ban bool) error {
	_, err := db.users.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"is_banned": ban}})
	return err
}

func (db *DB) countUsers(ctx context.Context) (int64, error) {
	return db.users.CountDocuments(ctx, bson.M{})
}

func (db *DB) getOrCreateApproval(ctx context.Context, deviceID, slug string) (*ApprovalRecord, bool, error) {
	var rec ApprovalRecord
	err := db.approvals.FindOne(ctx, bson.M{"device_id": deviceID}).Decode(&rec)
	if err == nil {
		return &rec, false, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, err
	}

	for attempt := 0; attempt < 8; attempt++ {
		candidate := 10000 + mrand.Intn(90000)
		newRec := ApprovalRecord{
			AccessID:  candidate,
			DeviceID:  deviceID,
			Slug:      slug,
			Approved:  false,
			Blocked:   false,
			CreatedAt: time.Now().UTC(),
		}
		_, insertErr := db.approvals.InsertOne(ctx, newRec)
		if insertErr == nil {
			return &newRec, true, nil
		}
		if !mongo.IsDuplicateKeyError(insertErr) {
			return nil, false, insertErr
		}
	}
	return nil, false, fmt.Errorf("could not allocate a unique access id")
}

func (db *DB) setApprovalName(ctx context.Context, deviceID, name string) error {
	_, err := db.approvals.UpdateOne(ctx, bson.M{"device_id": deviceID}, bson.M{"$set": bson.M{"visitor_name": name}})
	return err
}

func (db *DB) touchNotifyCooldown(ctx context.Context, deviceID string, cooldown time.Duration) (bool, error) {
	cutoff := time.Now().UTC().Add(-cooldown)
	filter := bson.M{
		"device_id": deviceID,
		"$or": []bson.M{
			{"last_notified_at": nil},
			{"last_notified_at": bson.M{"$exists": false}},
			{"last_notified_at": bson.M{"$lt": cutoff}},
		},
	}
	update := bson.M{"$set": bson.M{"last_notified_at": time.Now().UTC()}}
	res, err := db.approvals.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) getVisitorProfile(ctx context.Context, deviceID string) (*VisitorProfile, error) {
	p := &VisitorProfile{DeviceID: deviceID}
	err := db.visitorProfiles.FindOne(ctx, bson.M{"_id": deviceID}).Decode(p)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return p, nil
		}
		return nil, err
	}
	return p, nil
}

func (db *DB) upsertVisitorProfile(ctx context.Context, p *VisitorProfile) error {
	p.UpdatedAt = time.Now().UTC()
	filter := bson.M{"_id": p.DeviceID}
	update := bson.M{
		"$set": bson.M{
			"_id":        p.DeviceID,
			"name":       p.Name,
			"about":      p.About,
			"email":      p.Email,
			"phone":      p.Phone,
			"instagram":  p.Instagram,
			"facebook":   p.Facebook,
			"updated_at": p.UpdatedAt,
		},
	}
	_, err := db.visitorProfiles.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

func (db *DB) getVisitorProfileByAccessID(ctx context.Context, accessID int) (*ApprovalRecord, *VisitorProfile, error) {
	var rec ApprovalRecord
	err := db.approvals.FindOne(ctx, bson.M{"access_id": accessID}).Decode(&rec)
	if err != nil {
		return nil, nil, err
	}
	profile, err := db.getVisitorProfile(ctx, rec.DeviceID)
	return &rec, profile, err
}

func (db *DB) listApprovals(ctx context.Context, limit int) ([]*ApprovalRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(int64(limit))
	cursor, err := db.approvals.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*ApprovalRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) approveByID(ctx context.Context, accessID int) (bool, error) {
	now := time.Now().UTC()
	res, err := db.approvals.UpdateOne(ctx, bson.M{"access_id": accessID}, bson.M{"$set": bson.M{"approved": true, "approved_at": now}})
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) blockByID(ctx context.Context, accessID int, block bool) (bool, error) {
	res, err := db.approvals.UpdateOne(ctx, bson.M{"access_id": accessID}, bson.M{"$set": bson.M{"blocked": block}})
	if err != nil {
		return false, err
	}
	return res.MatchedCount > 0, nil
}

func (db *DB) deleteByAccessID(ctx context.Context, accessID int) (bool, error) {
	var rec ApprovalRecord
	err := db.approvals.FindOneAndDelete(ctx, bson.M{"access_id": accessID}).Decode(&rec)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	_, _ = db.visitorProfiles.DeleteOne(ctx, bson.M{"_id": rec.DeviceID})
	return true, nil
}

func (db *DB) deletePendingApprovals(ctx context.Context, olderThan *time.Duration) (int64, error) {
	filter := bson.M{"approved": false, "blocked": false}
	if olderThan != nil {
		cutoff := time.Now().UTC().Add(-*olderThan)
		filter["created_at"] = bson.M{"$lt": cutoff}
	}
	res, err := db.approvals.DeleteMany(ctx, filter)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

// ============================================================
// CACHE
// ============================================================

type Cache struct{ client *redis.Client }

type cachedFile struct {
	MessageID    int        `json:"message_id"`
	ChannelID    int64      `json:"channel_id"`
	FileName     string     `json:"file_name"`
	FileSize     int64      `json:"file_size"`
	MimeType     string     `json:"mime_type"`
	Hash         string     `json:"hash"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	PasswordHash *string    `json:"password_hash,omitempty"`
	GroupID      *string    `json:"group_id,omitempty"`
	QualityLabel string     `json:"quality_label,omitempty"`
	QualityRank  int        `json:"quality_rank,omitempty"`
}

func newCache(ctx context.Context, uri string) (*Cache, error) {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis uri: %w", err)
	}
	opts.DialTimeout = 10 * time.Second
	opts.ReadTimeout = 5 * time.Second
	opts.WriteTimeout = 5 * time.Second
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Cache{client: client}, nil
}

func (c *Cache) close() error { return c.client.Close() }

func (c *Cache) setFile(ctx context.Context, id string, f *cachedFile) {
	b, _ := json.Marshal(f)
	c.client.Set(ctx, "file:"+id, b, time.Hour)
}

func (c *Cache) getFile(ctx context.Context, id string) *cachedFile {
	b, err := c.client.Get(ctx, "file:"+id).Bytes()
	if err != nil {
		return nil
	}
	var f cachedFile
	if json.Unmarshal(b, &f) != nil {
		return nil
	}
	return &f
}

func (c *Cache) delFile(ctx context.Context, id string) { c.client.Del(ctx, "file:"+id) }

type adSettings struct {
	Enabled bool   `json:"enabled"`
	Type    string `json:"type"`
	URL     string `json:"url"`
}

const advertiseKey = "site:advertise"

func (c *Cache) setAdvertise(ctx context.Context, ad *adSettings) error {
	b, err := json.Marshal(ad)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, advertiseKey, b, 0).Err()
}

func (c *Cache) getAdvertise(ctx context.Context) *adSettings {
	b, err := c.client.Get(ctx, advertiseKey).Bytes()
	if err != nil {
		return nil
	}
	var ad adSettings
	if json.Unmarshal(b, &ad) != nil || !ad.Enabled {
		return nil
	}
	return &ad
}

func (c *Cache) clearAdvertise(ctx context.Context) { c.client.Del(ctx, advertiseKey) }

// --- Multi-quality upload session state ---
// A single admin can only have one active multi-upload session at a time.
// The session groups every file sent between /u and /d under one GroupID.

func (c *Cache) setMultiUploadSession(ctx context.Context, userID int64, groupID string) {
	c.client.Set(ctx, fmt.Sprintf("multiup:%d", userID), groupID, 30*time.Minute)
}

func (c *Cache) getMultiUploadSession(ctx context.Context, userID int64) (string, bool) {
	v, err := c.client.Get(ctx, fmt.Sprintf("multiup:%d", userID)).Result()
	if err != nil || v == "" {
		return "", false
	}
	return v, true
}

func (c *Cache) clearMultiUploadSession(ctx context.Context, userID int64) {
	c.client.Del(ctx, fmt.Sprintf("multiup:%d", userID))
}

func (c *Cache) addMultiUploadEntry(ctx context.Context, groupID, summary string) {
	key := fmt.Sprintf("multiup:files:%s", groupID)
	c.client.RPush(ctx, key, summary)
	c.client.Expire(ctx, key, 30*time.Minute)
}

func (c *Cache) clearMultiUploadEntries(ctx context.Context, groupID string) {
	c.client.Del(ctx, fmt.Sprintf("multiup:files:%s", groupID))
}

func (c *Cache) setAdvertisePending(ctx context.Context, userID int64) {
	c.client.Set(ctx, fmt.Sprintf("advertise:pending:%d", userID), "1", 5*time.Minute)
}
func (c *Cache) isAdvertisePending(ctx context.Context, userID int64) bool {
	v, err := c.client.Get(ctx, fmt.Sprintf("advertise:pending:%d", userID)).Result()
	return err == nil && v == "1"
}
func (c *Cache) clearAdvertisePending(ctx context.Context, userID int64) {
	c.client.Del(ctx, fmt.Sprintf("advertise:pending:%d", userID))
}

func (c *Cache) clearAllFileCache(ctx context.Context) {
	iter := c.client.Scan(ctx, 0, "file:*", 200).Iterator()
	for iter.Next(ctx) {
		c.client.Del(ctx, iter.Val())
	}
}

func (c *Cache) setFsub(ctx context.Context, userID int64, ok bool) {
	val := "0"
	if ok {
		val = "1"
	}
	c.client.Set(ctx, fmt.Sprintf("fsub:%d", userID), val, 5*time.Minute)
}

func (c *Cache) getFsub(ctx context.Context, userID int64) (ok, found bool) {
	val, err := c.client.Get(ctx, fmt.Sprintf("fsub:%d", userID)).Result()
	if err != nil {
		return false, false
	}
	return val == "1", true
}

func (c *Cache) delFsub(ctx context.Context, userID int64) {
	c.client.Del(ctx, fmt.Sprintf("fsub:%d", userID))
}

const liveWindowSecs = 30

func (c *Cache) heartbeat(ctx context.Context, slug, deviceID string) {
	now := float64(time.Now().Unix())
	cutoff := fmt.Sprintf("%f", now-liveWindowSecs)

	key := "live:" + slug
	c.client.ZAdd(ctx, key, redis.Z{Score: now, Member: deviceID})
	c.client.ZRemRangeByScore(ctx, key, "-inf", cutoff)
	c.client.Expire(ctx, key, 2*time.Minute)

	gkey := "live:__all__"
	c.client.ZAdd(ctx, gkey, redis.Z{Score: now, Member: slug + ":" + deviceID})
	c.client.ZRemRangeByScore(ctx, gkey, "-inf", cutoff)
	c.client.Expire(ctx, gkey, 2*time.Minute)
}

func (c *Cache) liveCount(ctx context.Context, slug string) int64 {
	now := float64(time.Now().Unix())
	key := "live:" + slug
	c.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", now-liveWindowSecs))
	n, _ := c.client.ZCard(ctx, key).Result()
	return n
}

func (c *Cache) liveCountAll(ctx context.Context) int64 {
	now := float64(time.Now().Unix())
	key := "live:__all__"
	c.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", now-liveWindowSecs))
	n, _ := c.client.ZCard(ctx, key).Result()
	return n
}

// ============================================================
// BOT POOL
// ============================================================

type BotPool struct {
	bots   []*tgbotapi.BotAPI
	index  atomic.Uint64
	mu     sync.RWMutex
	logger *zap.Logger
}

func newBotPool(tokens []string, logger *zap.Logger) (*BotPool, error) {
	p := &BotPool{logger: logger}
	for i, token := range tokens {
		bot, err := tgbotapi.NewBotAPI(token)
		if err != nil {
			return nil, fmt.Errorf("bot %d: %w", i+1, err)
		}
		p.bots = append(p.bots, bot)
		logger.Info("bot ready", zap.String("username", "@"+bot.Self.UserName))
	}
	return p, nil
}

func (p *BotPool) primary() *tgbotapi.BotAPI {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.bots[0]
}

func (p *BotPool) next() *tgbotapi.BotAPI {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.bots[int(p.index.Add(1)-1)%len(p.bots)]
}

func (p *BotPool) count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.bots)
}

func (p *BotPool) isMember(channelID, userID int64) (bool, error) {
	m, err := p.primary().GetChatMember(tgbotapi.GetChatMemberConfig{
		ChatConfigWithUser: tgbotapi.ChatConfigWithUser{ChatID: channelID, UserID: userID},
	})
	if err != nil {
		return false, err
	}
	s := m.Status
	return s == "creator" || s == "administrator" || s == "member" || s == "restricted", nil
}

func (p *BotPool) send(chatID int64, text string) {
	p.primary().Send(tgbotapi.NewMessage(chatID, text))
}

func (p *BotPool) sendMD(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "MarkdownV2"
	p.primary().Send(msg)
}

func (p *BotPool) sendKB(chatID int64, text string, kb tgbotapi.InlineKeyboardMarkup) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "MarkdownV2"
	msg.ReplyMarkup = kb
	p.primary().Send(msg)
}

func (p *BotPool) delMsg(chatID int64, msgID int) {
	p.primary().Request(tgbotapi.NewDeleteMessage(chatID, msgID))
}

func (p *BotPool) stopUpdates() { p.primary().StopReceivingUpdates() }

func calculateBlockSize(start, end int64) int64 {
	size := end - start + 1
	switch {
	case size < 512*1024:
		return 64 * 1024
	case size < 4*1024*1024:
		return 256 * 1024
	case size < 32*1024*1024:
		return 512 * 1024
	default:
		return 1024 * 1024
	}
}

type MTProtoPool struct {
	bots   []*mtBot
	index  atomic.Uint64
	mu     sync.RWMutex
	logger *zap.Logger
}

type mtBot struct {
	client      *telegram.Client
	api         *tg.Client
	token       string
	isUser      bool
	sessionData []byte
	ready       bool
	mu          sync.Mutex
}

type stringSessionStorage struct {
	mu   sync.Mutex
	data []byte
}

func (s *stringSessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return nil, fmt.Errorf("string session: no data loaded")
	}
	return s.data, nil
}

func (s *stringSessionStorage) StoreSession(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
	return nil
}

func (b *mtBot) isReady() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready
}

func newMTProtoPool(apiID int, apiHash string, tokens []string, stringSession string, logger *zap.Logger) *MTProtoPool {
	pool := &MTProtoPool{logger: logger}
	for _, token := range tokens {
		pool.bots = append(pool.bots, &mtBot{token: token})
	}

	if stringSession != "" {
		data, err := base64.StdEncoding.DecodeString(stringSession)
		if err != nil {
			logger.Warn("STRING_SESSION invalid base64", zap.Error(err))
		} else {
			pool.bots = append(pool.bots, &mtBot{isUser: true, sessionData: data})
		}
	}

	for i, bot := range pool.bots {
		go func(idx int, b *mtBot) {
			pool.startBot(context.Background(), apiID, apiHash, b)
		}(i, bot)
	}
	return pool
}

func getFloodMiddleware() []telegram.Middleware {
	waiter := floodwait.NewSimpleWaiter().WithMaxRetries(10)
	limiter := ratelimit.New(rate.Every(100*time.Millisecond), 5)
	return []telegram.Middleware{waiter, limiter}
}

func (p *MTProtoPool) startBot(ctx context.Context, apiID int, apiHash string, b *mtBot) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opts := telegram.Options{
			DCList:      dcs.Prod(),
			Logger:      p.logger.Named("mtproto"),
			Middlewares: getFloodMiddleware(),
		}
		if b.isUser {
			opts.SessionStorage = &stringSessionStorage{data: b.sessionData}
		}
		client := telegram.NewClient(apiID, apiHash, opts)

		err := client.Run(ctx, func(ctx context.Context) error {
			if b.isUser {
				status, err := client.Auth().Status(ctx)
				if err != nil {
					return fmt.Errorf("string session status check failed: %w", err)
				}
				if !status.Authorized {
					return fmt.Errorf("STRING_SESSION expired ya invalid hai")
				}
			} else {
				if _, err := client.Auth().Bot(ctx, b.token); err != nil {
					return fmt.Errorf("bot auth failed: %w", err)
				}
			}

			b.mu.Lock()
			b.client = client
			b.api = tg.NewClient(client)
			b.ready = true
			b.mu.Unlock()

			if b.isUser {
				p.logger.Info("MTProto userbot authenticated")
			} else {
				p.logger.Info("MTProto bot authenticated")
			}

			<-ctx.Done()
			return nil
		})

		b.mu.Lock()
		b.ready = false
		b.mu.Unlock()

		if err != nil && ctx.Err() == nil {
			if b.isUser {
				p.logger.Warn("STRING_SESSION worker stopped", zap.Error(err))
				return
			}
			p.logger.Warn("MTProto reconnecting...", zap.Error(err))
			time.Sleep(5 * time.Second)
		} else {
			return
		}
	}
}

func (p *MTProtoPool) next() *mtBot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := int(p.index.Add(1)-1) % len(p.bots)
	return p.bots[n]
}

func (p *MTProtoPool) isAnyReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, b := range p.bots {
		if b.isReady() {
			return true
		}
	}
	return false
}

func (p *MTProtoPool) getFileLocation(ctx context.Context, channelID int64, messageID int) (*tg.InputDocumentFileLocation, int64, error) {
	bot := p.next()
	if !bot.isReady() {
		return nil, 0, fmt.Errorf("MTProto bot not ready")
	}

	bot.mu.Lock()
	api := bot.api
	bot.mu.Unlock()

	inputChan := &tg.InputChannel{ChannelID: channelID}
	result, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{inputChan})
	if err != nil {
		return nil, 0, fmt.Errorf("get channel: %w", err)
	}

	var accessHash int64
	if chats, ok := result.(*tg.MessagesChats); ok {
		for _, chat := range chats.Chats {
			if ch, ok := chat.(*tg.Channel); ok && ch.ID == channelID {
				accessHash = ch.AccessHash
				break
			}
		}
	}

	inputChan.AccessHash = accessHash

	msgs, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: inputChan,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: messageID}},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("get message: %w", err)
	}

	var messages []tg.MessageClass
	switch m := msgs.(type) {
	case *tg.MessagesMessages:
		messages = m.Messages
	case *tg.MessagesMessagesSlice:
		messages = m.Messages
	case *tg.MessagesChannelMessages:
		messages = m.Messages
	}

	for _, msg := range messages {
		m, ok := msg.(*tg.Message)
		if !ok {
			continue
		}
		media, ok := m.Media.(*tg.MessageMediaDocument)
		if !ok {
			continue
		}
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			continue
		}
		return &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
		}, doc.Size, nil
	}

	return nil, 0, fmt.Errorf("no document in message %d", messageID)
}

type TgFileReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	api        *tg.Client
	cfg        *Config
	location   *tg.InputDocumentFileLocation
	start      int64
	end        int64
	blockSize  int64
	totalBytes int64

	blockQueue   chan []byte
	currentBlock []byte
	blockOffset  int64
	bytesRead    int64

	closeOnce sync.Once
}

func newTgFileReader(ctx context.Context, api *tg.Client, cfg *Config,
	location *tg.InputDocumentFileLocation, fileSize, start, end int64) *TgFileReader {

	ctx, cancel := context.WithCancel(ctx)
	blockSize := calculateBlockSize(start, end)

	r := &TgFileReader{
		ctx:        ctx,
		cancel:     cancel,
		api:        api,
		cfg:        cfg,
		location:   location,
		start:      start,
		end:        end,
		blockSize:  blockSize,
		totalBytes: end - start + 1,
		blockQueue: make(chan []byte, cfg.StreamBufferCount),
	}
	go r.prefetch()
	return r
}

func (r *TgFileReader) Close() {
	r.closeOnce.Do(func() { r.cancel() })
}

func (r *TgFileReader) Read(p []byte) (n int, err error) {
	if r.bytesRead >= r.totalBytes {
		return 0, io.EOF
	}

	if r.blockOffset >= int64(len(r.currentBlock)) {
		select {
		case block, ok := <-r.blockQueue:
			if !ok {
				if r.bytesRead >= r.totalBytes {
					return 0, io.EOF
				}
				return 0, fmt.Errorf("pipe drained")
			}
			r.currentBlock = block
			r.blockOffset = 0
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}

	n = copy(p, r.currentBlock[r.blockOffset:])
	r.blockOffset += int64(n)
	r.bytesRead += int64(n)
	return n, nil
}

func (r *TgFileReader) prefetch() {
	defer close(r.blockQueue)

	alignedStart := r.start - (r.start % r.blockSize)
	leftTrim     := r.start - alignedStart
	rightTrim    := (r.end % r.blockSize) + 1
	totalBlocks  := int((r.end - alignedStart + r.blockSize) / r.blockSize)

	currentBlock := 0
	offset       := alignedStart

	for currentBlock < totalBlocks {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		batchSize := r.cfg.StreamConcurrency
		if batchSize > totalBlocks - currentBlock {
			batchSize = totalBlocks - currentBlock
		}
		blocks := make([][]byte, batchSize)

		var wg sync.WaitGroup
		var fetchErr error
		var errMu sync.Mutex

		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				blockNum    := currentBlock + idx
				blockOffset := offset + int64(idx)*r.blockSize

				data, err := r.downloadWithRetry(blockOffset)
				if err != nil {
					errMu.Lock()
					if fetchErr == nil { fetchErr = err }
					errMu.Unlock()
					return
				}

				dataLen := int64(len(data))
				if totalBlocks == 1 {
					if rightTrim > dataLen { rightTrim = dataLen }
					if leftTrim  > dataLen { leftTrim  = dataLen }
					data = data[leftTrim:rightTrim]
				} else if blockNum == 0 {
					if leftTrim > dataLen { leftTrim = dataLen }
					data = data[leftTrim:]
				} else if blockNum == totalBlocks-1 {
					if dataLen > rightTrim { data = data[:rightTrim] }
				}
				blocks[idx] = data
			}(i)
		}
		wg.Wait()

		if fetchErr != nil && r.ctx.Err() == nil {
			return
		}

		for _, block := range blocks {
			if block == nil { return }
			select {
			case r.blockQueue <- block:
			case <-r.ctx.Done():
				return
			}
		}

		currentBlock += batchSize
		offset       += r.blockSize * int64(batchSize)
	}
}

func (r *TgFileReader) downloadWithRetry(offset int64) ([]byte, error) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 15 * time.Second
	var lastErr error

	for attempt := 0; attempt < r.cfg.StreamMaxRetries; attempt++ {
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}

		timeout := time.Duration(r.cfg.StreamTimeoutSec) * time.Second
		ctx, cancel := context.WithTimeout(r.ctx, timeout)
		data, err := r.downloadBlock(ctx, offset)
		cancel()

		if err == nil {
			return data, nil
		}
		lastErr = err

		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff { backoff = maxBackoff }
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (r *TgFileReader) downloadBlock(ctx context.Context, offset int64) ([]byte, error) {
	res, err := r.api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Location: r.location,
		Offset:   offset,
		Limit:    int(r.blockSize),
	})
	if err != nil {
		return nil, err
	}
	switch result := res.(type) {
	case *tg.UploadFile:
		return result.Bytes, nil
	default:
		return nil, fmt.Errorf("unexpected response: %T", res)
	}
}

// ============================================================
// APP
// ============================================================

type App struct {
	cfg    *Config
	db     *DB
	cache  *Cache
	pool   *BotPool
	mtPool *MTProtoPool
	logger *zap.Logger
}

func (a *App) dispatch(ctx context.Context, update tgbotapi.Update) {
	switch {
	case update.Message != nil:
		a.onMessage(ctx, update.Message)
	case update.CallbackQuery != nil:
		a.onCallback(ctx, update.CallbackQuery)
	}
}

func (a *App) onMessage(ctx context.Context, msg *tgbotapi.Message) {
	if !msg.Chat.IsPrivate() {
		return
	}
	userID := msg.From.ID

	if userID != a.cfg.AdminID {
		a.pool.send(msg.Chat.ID, "🔒 Private Bot\n\nYeh bot sirf personal use ke liye hai.")
		return
	}

	_ = a.db.upsertUser(ctx, &UserRecord{
		ID: userID, Username: msg.From.UserName, FirstName: msg.From.FirstName,
	})

	user, err := a.db.getUser(ctx, userID)
	if err == nil && user.IsBanned {
		a.pool.send(msg.Chat.ID, "⛔ You are banned.")
		return
	}

	if a.cfg.MainChannelID != 0 {
		ok, found := a.cache.getFsub(ctx, userID)
		if !found {
			ok, _ = a.pool.isMember(a.cfg.MainChannelID, userID)
			a.cache.setFsub(ctx, userID, ok)
		}
		if !ok {
			a.sendFsubPrompt(msg.Chat.ID)
			return
		}
	}

	switch {
	case msg.IsCommand():
		a.onCommand(ctx, msg)
	case msg.Document != nil || msg.Video != nil || msg.Audio != nil ||
		msg.Voice != nil || msg.VideoNote != nil || len(msg.Photo) > 0:
		a.onFile(ctx, msg)
	default:
		a.pool.send(msg.Chat.ID, "📎 Send me any file to get a permanent streaming link!")
	}
}

func (a *App) onCommand(ctx context.Context, msg *tgbotapi.Message) {
	switch msg.Command() {
	case "start":
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"👋 Hello *%s*\\!\n\nSend me any file \\(any size\\!\\) and get a permanent streaming link\\.\n\nWorks in Chrome, Firefox and VLC\\!",
			mdEscape(msg.From.FirstName),
		))
	case "help":
		a.pool.sendMD(msg.Chat.ID,
			"*Commands:*\n/start \\- Welcome\n/help \\- Help\n/u \\- Start multi\\-quality upload session \\(admin\\)\n/d \\- Finish multi\\-quality upload, get single link \\(admin\\)\n/stats \\- Stats \\(admin\\)\n/expire \\- Set/remove link expiry \\(admin\\)\n/setpass \\- Password\\-protect a link \\(uploader/admin\\)\n/tag \\- Tag a file with Subject/Chapter \\(admin\\)\n/untag \\- Remove a file's Subject/Chapter tag \\(admin\\)\n/setyear \\- Tag a file with a Year 1930\\-2030 \\(admin\\)\n/setepisode \\- Tag a file with a Season/Episode/Part label \\(admin\\)\n/approve \\- Approve a visitor's Access ID \\(admin\\)\n/block \\- Block a visitor's Access ID \\(admin\\)\n/unblock \\- Unblock a visitor's Access ID \\(admin\\)\n/reject \\- Delete a visitor's Access ID completely \\(admin\\)\n/user \\- List recent visitors and their Access IDs \\(admin\\)\n/profile \\- View a visitor's full profile by Access ID \\(admin\\)\n/clearpending \\- Delete all pending visitors \\(admin\\)\n/dashboard \\- Get admin dashboard link \\(admin\\)\n/advertise \\- Set the watch\\-page ad banner \\(admin\\)\n/dminem \\- Delete ALL files \\(admin, asks confirmation\\)\n\nSend any file to get a link\\!\n\nBy default links are *permanent*\\. Use /expire \\<file\\_id\\> \\<time\\> to make one expire \\(e\\.g\\. `7d`, `12h`, `1y`, or `off` to remove it\\)\\.")
	case "u":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		groupID := uuid.New().String()
		a.cache.setMultiUploadSession(ctx, msg.From.ID, groupID)
		a.pool.send(msg.Chat.ID,
			"📦 Multi-Quality Upload session shuru!\n\nAb saari quality wali files bhejo (jaise Movie.480p.mkv, Movie.720p.mkv, Movie.1080p.mkv) — filename mein quality zaroor likhi honi chahiye taaki bot use pehchaan sake.\n\nJab sab bhej do, /d bhejo — ek hi link banega jisme quality switch karne ka option hoga.")
	case "d":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		groupID, active := a.cache.getMultiUploadSession(ctx, msg.From.ID)
		if !active {
			a.pool.send(msg.Chat.ID, "⚠️ Koi active multi-upload session nahi hai. Pehle /u bhejo.")
			return
		}
		a.cache.clearMultiUploadSession(ctx, msg.From.ID)
		files, ferr := a.db.listGroupFiles(ctx, groupID)
		if ferr != nil || len(files) == 0 {
			a.cache.clearMultiUploadEntries(ctx, groupID)
			a.pool.send(msg.Chat.ID, "⚠️ Is session mein koi file nahi mili. Cancel kar diya — /u se dobara shuru karo.")
			return
		}
		primary := files[0]
		for _, f := range files {
			if f.QualityRank > primary.QualityRank {
				primary = f
			}
		}
		base := a.cfg.baseURL()
		streamLink := fmt.Sprintf("%s/stream/%s", base, primary.ID)
		dlLink := fmt.Sprintf("%s/dl/%s", base, primary.ID)
		watchLink := fmt.Sprintf("%s/watch/%s", base, primary.ID)

		var qLines strings.Builder
		for _, f := range files {
			l := f.QualityLabel
			if l == "" {
				l = "Unknown"
			}
			qLines.WriteString(fmt.Sprintf("• %s (%s)\n", l, formatSize(f.FileSize)))
		}

		text := fmt.Sprintf(
			"✅ *Multi\\-Quality Group Ready\\!*\n\n📄 `%s`\n🆔 `%s`\n\n📊 *Qualities:*\n%s\n▶️ [Stream](%s)\n⬇️ [Download](%s)\n📺 [Watch Online](%s)\n\n_Watch page pe quality switch kar sakte ho\\!_",
			mdEscape(primary.FileName), primary.ID, mdEscape(qLines.String()), streamLink, dlLink, watchLink,
		)
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("▶️ Stream", streamLink),
				tgbotapi.NewInlineKeyboardButtonURL("⬇️ Download", dlLink),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("📺 Watch Online", watchLink),
			),
		)
		a.pool.sendKB(msg.Chat.ID, text, kb)
		a.cache.clearMultiUploadEntries(ctx, groupID)

		if a.cfg.LogChannelID != 0 {
			a.pool.sendMD(a.cfg.LogChannelID, fmt.Sprintf(
				"📁 *New Multi\\-Quality Group*\n👤 [%s](tg://user?id=%d)\n📄 `%s`\n🔗 %s",
				mdEscape(msg.From.FirstName), msg.From.ID,
				mdEscape(primary.FileName), streamLink,
			))
		}
	case "stats":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		files, _ := a.db.countFiles(ctx)
		users, _ := a.db.countUsers(ctx)
		totalViews, _ := a.db.sumViews(ctx)
		live := a.cache.liveCountAll(ctx)
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"📊 *Stats*\n\n📁 Files: `%d`\n👥 Users: `%d`\n🤖 Bots: `%d`\n👁 Total unique views: `%d`\n🔴 Live now: `%d`",
			files, users, a.pool.count(), totalViews, live,
		))
	case "dashboard":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		link := fmt.Sprintf("%s/admin?token=%s", a.cfg.baseURL(), a.cfg.DashboardToken)
		a.pool.send(msg.Chat.ID, fmt.Sprintf(
			"🖥️ Admin Dashboard\n\n%s\n\n⚠️ Yeh link kisi ko share mat karna — jiske paas yeh link hai woh dashboard dekh sakta hai.\n\n🆔 Kisi bhi watch page pe visitor-lookup icon dikhane ke liye, link ke aakhir mein ?admin=%s laga do.",
			link, a.cfg.DashboardToken,
		))
	case "advertise":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		arg := strings.TrimSpace(msg.CommandArguments())
		switch {
		case strings.EqualFold(arg, "off"):
			a.cache.clearAdvertise(ctx)
			a.pool.send(msg.Chat.ID, "✅ Advertise banner hata diya gaya — watch page pehle jaisa dikhega.")
		case arg == "":
			a.cache.setAdvertisePending(ctx, msg.From.ID)
			a.pool.send(msg.Chat.ID,
				"📸 Theek hai — ab agla photo ya video jo tum bhejoge (5 min ke andar), wahi watch page ke top par advertise banner ban jaayega.\n\nYa seedha URL bhi de sakte ho:\n/advertise <image ya video ka URL>\n\nHatane ke liye: /advertise off")
		default:
			adType := "image"
			lower := strings.ToLower(arg)
			for _, ext := range []string{".mp4", ".webm", ".mov", ".mkv", ".m3u8"} {
				if strings.HasSuffix(lower, ext) {
					adType = "video"
					break
				}
			}
			if err := a.cache.setAdvertise(ctx, &adSettings{Enabled: true, Type: adType, URL: arg}); err != nil {
				a.pool.send(msg.Chat.ID, "❌ Advertise set nahi ho paya, dobara try karo.")
				return
			}
			a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
				"✅ Advertise banner set ho gaya \\(%s\\)\\.\n\nWatch page ke top par ab yeh highlight hoke dikhega\\.", adType))
		}
	case "dminem":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		count, _ := a.db.countFiles(ctx)
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("⚠️ Haan, SAB delete karo", "confirm_delete_all"),
				tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", "cancel_delete_all"),
			),
		)
		a.pool.sendKB(msg.Chat.ID, fmt.Sprintf(
			"⚠️ Yeh *%d* files ko *PERMANENTLY* delete kar dega \\— sabhi links kaam karna band ho jaayenge\\.\n\nPakka delete karna hai?",
			count,
		), kb)
	case "setpass":
		parts := strings.Fields(msg.CommandArguments())
		if len(parts) < 2 {
			a.pool.send(msg.Chat.ID,
				"Usage: /setpass <file_id> <password>\nOr: /setpass <file_id> off   (remove password)")
			return
		}
		slug := parts[0]
		rec, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ File not found.")
			return
		}
		if msg.From.ID != a.cfg.AdminID && msg.From.ID != rec.UploaderID {
			a.pool.send(msg.Chat.ID, "❌ Sirf uploader ya admin password set kar sakta hai.")
			return
		}
		if strings.EqualFold(parts[1], "off") {
			if err := a.db.setPassword(ctx, slug, nil, nil); err != nil {
				a.pool.send(msg.Chat.ID, "❌ Failed to remove password.")
				return
			}
			a.cache.delFile(ctx, slug)
			a.pool.send(msg.Chat.ID, "✅ Password removed — link ab bina password ke khulega.")
			return
		}
		password := strings.Join(parts[1:], " ")
		hash := sha256Hex(password)
		if err := a.db.setPassword(ctx, slug, &hash, &password); err != nil {
			a.pool.send(msg.Chat.ID, "❌ Failed to set password.")
			return
		}
		a.cache.delFile(ctx, slug)
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"🔒 Password set for `%s`\\.\n\nAb koi bhi is link ko kholega, pehle yeh password daalna hoga\\.",
			mdEscape(slug),
		))
	case "tag":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		args := strings.TrimSpace(msg.CommandArguments())
		parts := strings.SplitN(args, " ", 2)
		if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
			a.pool.send(msg.Chat.ID,
				"Usage: /tag <file_id> <Subject>\nYa: /tag <file_id> <Subject> / <Chapter>\n\nExample:\n/tag abc123 Physics\n/tag abc123 Physics / Chapter 3 - Motion")
			return
		}
		fileID := parts[0]
		rest := strings.TrimSpace(parts[1])
		subject, chapter := rest, ""
		if idx := strings.Index(rest, "/"); idx != -1 {
			subject = strings.TrimSpace(rest[:idx])
			chapter = strings.TrimSpace(rest[idx+1:])
		}
		if subject == "" {
			a.pool.send(msg.Chat.ID, "❌ Subject khali nahi ho sakta.")
			return
		}
		found, err := a.db.setFileTag(ctx, fileID, subject, chapter)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Tag failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
			return
		}
		label := subject
		if chapter != "" {
			label = subject + " / " + chapter
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf("🏷️ Tagged as *%s*\\.", mdEscape(label)))
	case "untag":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		fileID := strings.TrimSpace(msg.CommandArguments())
		if fileID == "" {
			a.pool.send(msg.Chat.ID, "Usage: /untag <file_id>")
			return
		}
		found, err := a.db.setFileTag(ctx, fileID, "", "")
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Untag failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
			return
		}
		a.pool.send(msg.Chat.ID, "✅ Tag hata diya.")
	case "setyear":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		args := strings.TrimSpace(msg.CommandArguments())
		parts := strings.SplitN(args, " ", 2)
		if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
			a.pool.send(msg.Chat.ID,
				"Usage: /setyear <file_id> <year>\n\nExample:\n/setyear abc123 1994\n/setyear abc123 0")
			return
		}
		fileID := parts[0]
		year, convErr := strconv.Atoi(strings.TrimSpace(parts[1]))
		if convErr != nil || (year != 0 && (year < 1930 || year > 2030)) {
			a.pool.send(msg.Chat.ID, "❌ Year 1930 se 2030 ke beech ek number hona chahiye (ya 0 clear karne ke liye).")
			return
		}
		found, err := a.db.setFileYear(ctx, fileID, year)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Set year failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
			return
		}
		if year == 0 {
			a.pool.send(msg.Chat.ID, "✅ Year hata diya.")
		} else {
			a.pool.sendMD(msg.Chat.ID, fmt.Sprintf("📅 Year set: *%d*\\.", year))
		}
	case "setepisode":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		args := strings.TrimSpace(msg.CommandArguments())
		parts := strings.SplitN(args, " ", 2)
		if len(parts) < 1 || strings.TrimSpace(parts[0]) == "" {
			a.pool.send(msg.Chat.ID,
				"Usage: /setepisode <file_id> <label>\n\nExample:\n/setepisode abc123 Season 2 Episode 5\n/setepisode abc123 Part 3")
			return
		}
		fileID := parts[0]
		label := ""
		if len(parts) == 2 {
			label = strings.TrimSpace(parts[1])
		}
		found, err := a.db.setFileEpisode(ctx, fileID, label)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Set episode failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
			return
		}
		if label == "" {
			a.pool.send(msg.Chat.ID, "✅ Episode/Part label hata diya.")
		} else {
			a.pool.sendMD(msg.Chat.ID, fmt.Sprintf("🎬 Episode/Part set: *%s*\\.", mdEscape(label)))
		}
	case "user":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		list, err := a.db.listApprovals(ctx, 30)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Failed to load visitors.")
			return
		}
		if len(list) == 0 {
			a.pool.send(msg.Chat.ID, "Abhi tak koi visitor nahi aaya.")
			return
		}
		var b strings.Builder
		b.WriteString("👥 *Recent Visitors*\n\n")
		for _, v := range list {
			name := v.VisitorName
			if name == "" {
				name = "—"
			}
			status := "⏳ pending"
			if v.Approved {
				status = "✅ approved"
			}
			if v.Blocked {
				status = "🚫 blocked"
			}
			fmt.Fprintf(&b, "🆔 `%05d` \\— %s \\(%s\\)\n", v.AccessID, mdEscape(name), status)
		}
		a.pool.sendMD(msg.Chat.ID, b.String())
	case "profile":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /profile <access_id>")
			return
		}
		rec, profile, err := a.db.getVisitorProfileByAccessID(ctx, accessID)
		if err != nil {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		status := "⏳ pending"
		if rec.Approved {
			status = "✅ approved"
		}
		if rec.Blocked {
			status = "🚫 blocked"
		}
		name := rec.VisitorName
		if name == "" {
			name = "—"
		}
		var b strings.Builder
		fmt.Fprintf(&b, "🆔 *Access ID:* `%05d`\n*Status:* %s\n*Name:* %s\n", rec.AccessID, status, mdEscape(name))
		if profile != nil {
			if profile.About != "" {
				fmt.Fprintf(&b, "*About:* %s\n", mdEscape(profile.About))
			}
			if profile.Email != "" {
				fmt.Fprintf(&b, "📧 %s\n", mdEscape(profile.Email))
			}
			if profile.Phone != "" {
				fmt.Fprintf(&b, "📱 %s\n", mdEscape(profile.Phone))
			}
			if profile.Instagram != "" {
				fmt.Fprintf(&b, "📸 %s\n", mdEscape(profile.Instagram))
			}
			if profile.Facebook != "" {
				fmt.Fprintf(&b, "👤 %s\n", mdEscape(profile.Facebook))
			}
			if profile.Email == "" && profile.Phone == "" && profile.Instagram == "" && profile.Facebook == "" && profile.About == "" {
				b.WriteString("\n_Is visitor ne apna profile abhi tak nahi bhara\\._")
			}
		}
		a.pool.sendMD(msg.Chat.ID, b.String())
	case "approve":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /approve <access_id>")
			return
		}
		found, err := a.db.approveByID(ctx, accessID)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Approve failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"✅ Access ID `%05d` approve ho gaya\\. Uska page apne aap unlock ho jaayega\\.",
			accessID,
		))
	case "ban":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		var id int64
		if _, err := fmt.Sscanf(msg.CommandArguments(), "%d", &id); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /ban <user_id>")
			return
		}
		_ = a.db.banUser(ctx, id, true)
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ User %d banned.", id))
	case "unban":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		var id int64
		if _, err := fmt.Sscanf(msg.CommandArguments(), "%d", &id); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /unban <user_id>")
			return
		}
		_ = a.db.banUser(ctx, id, false)
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ User %d unbanned.", id))
	case "block":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /block <access_id>")
			return
		}
		found, err := a.db.blockByID(ctx, accessID, true)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Block failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"🚫 Access ID `%05d` block ho gaya\\. Yeh device ab kisi bhi link ko access nahi kar paayega\\.",
			accessID,
		))
	case "unblock":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /unblock <access_id>")
			return
		}
		found, err := a.db.blockByID(ctx, accessID, false)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Unblock failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf("✅ Access ID `%05d` unblock ho gaya\\.", accessID))
	case "reject":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /reject <access_id>")
			return
		}
		found, err := a.db.deleteByAccessID(ctx, accessID)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Reject failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"🗑️ Access ID `%05d` completely delete ho gaya\\. Yeh visitor ab bilkul naye sirre se aayega\\.",
			accessID,
		))
	case "clearpending":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		n, err := a.db.deletePendingApprovals(ctx, nil)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Clear failed, dobara try karo.")
			return
		}
		a.pool.send(msg.Chat.ID, fmt.Sprintf("🧹 %d pending visitor(s) clear ho gaye. Approved aur blocked IDs safe hain.", n))
	case "delete":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		slug := strings.TrimSpace(msg.CommandArguments())
		if slug == "" {
			a.pool.send(msg.Chat.ID, "Usage: /delete <file_id>")
			return
		}
		_, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ File not found.")
			return
		}
		found, err := a.db.deleteFileByID(ctx, slug)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Delete failed — DB error, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ Delete failed — file DB mein nahi mila (ho sakta hai already delete ho chuka ho).")
			return
		}
		a.cache.delFile(ctx, slug)
		a.pool.send(msg.Chat.ID, "✅ File deleted.")
	case "expire":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		parts := strings.Fields(msg.CommandArguments())
		if len(parts) != 2 {
			a.pool.send(msg.Chat.ID,
				"Usage: /expire <file_id> <duration>\n\nExamples:\n/expire abc123 30m\n/expire abc123 12h\n/expire abc123 7d\n/expire abc123 1y\n/expire abc123 off")
			return
		}
		slug, durStr := parts[0], parts[1]

		if _, err := a.db.getFileByID(ctx, slug); err != nil {
			a.pool.send(msg.Chat.ID, "❌ File not found.")
			return
		}

		dur, clear, err := parseExpiryDuration(durStr)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ "+err.Error())
			return
		}

		if clear {
			if err := a.db.setExpiry(ctx, slug, nil); err != nil {
				a.pool.send(msg.Chat.ID, "❌ Failed to update expiry.")
				return
			}
			a.cache.delFile(ctx, slug)
			a.pool.send(msg.Chat.ID, "✅ Expiry removed — link is permanent (unlimited) again.")
			return
		}

		expiresAt := time.Now().Add(dur)
		if err := a.db.setExpiry(ctx, slug, &expiresAt); err != nil {
			a.pool.send(msg.Chat.ID, "❌ Failed to set expiry.")
			return
		}
		a.cache.delFile(ctx, slug)
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"✅ Expiry set for `%s`\\.\n\n⏳ Link expires: *%s*",
			mdEscape(slug), mdEscape(expiresAt.Format("02 Jan 2006, 15:04 MST")),
		))
	}
}

func (a *App) storeAdvertiseMedia(ctx context.Context, msg *tgbotapi.Message) (streamURL, adType string, err error) {
	var fileName, mimeType string
	var fileSize int64
	switch {
	case msg.Video != nil:
		v := msg.Video
		fileName = v.FileName
		if fileName == "" {
			fileName = "ad_video_" + v.FileUniqueID[:8] + ".mp4"
		}
		fileSize, mimeType, adType = int64(v.FileSize), "video/mp4", "video"
	case len(msg.Photo) > 0:
		ph := msg.Photo[len(msg.Photo)-1]
		fileName = "ad_photo_" + ph.FileUniqueID[:8] + ".jpg"
		fileSize, mimeType, adType = int64(ph.FileSize), "image/jpeg", "image"
	default:
		return "", "", fmt.Errorf("unsupported media for advertise")
	}

	fwdMsg, err := a.pool.next().Send(tgbotapi.NewForward(a.cfg.DBChannelID, msg.Chat.ID, msg.MessageID))
	if err != nil {
		return "", "", fmt.Errorf("forward failed: %w", err)
	}

	hash := makeShortHash(fileName, fileSize, fwdMsg.MessageID)
	slug := uuid.New().String()
	channelID := toInternalChannelID(a.cfg.DBChannelID)

	rec := &FileRecord{
		ID:           slug,
		MessageID:    fwdMsg.MessageID,
		ChannelID:    channelID,
		FileName:     fileName,
		FileSize:     fileSize,
		MimeType:     mimeType,
		Hash:         hash,
		UploaderID:   msg.From.ID,
		UploaderName: msg.From.UserName,
	}
	if err := a.db.saveFile(ctx, rec); err != nil {
		return "", "", fmt.Errorf("db save failed: %w", err)
	}
	a.cache.setFile(ctx, slug, &cachedFile{
		MessageID: fwdMsg.MessageID,
		ChannelID: channelID,
		FileName:  fileName,
		FileSize:  fileSize,
		MimeType:  mimeType,
		Hash:      hash,
	})

	streamURL = fmt.Sprintf("%s/stream/%s", a.cfg.baseURL(), slug)
	return streamURL, adType, nil
}

type fInfo struct {
	fileName string
	fileSize int64
	mimeType string
}

// extractFileInfo pulls a display filename/size/mimetype out of whichever
// media field is populated on the message. ok is false for unsupported types.
func extractFileInfo(msg *tgbotapi.Message) (fi fInfo, ok bool) {
	switch {
	case msg.Document != nil:
		d := msg.Document
		name := d.FileName
		if name == "" {
			name = "document_" + d.FileUniqueID[:8]
		}
		fi = fInfo{name, int64(d.FileSize), d.MimeType}
	case msg.Video != nil:
		v := msg.Video
		name := v.FileName
		if name == "" {
			name = "video_" + v.FileUniqueID[:8] + ".mp4"
		}
		fi = fInfo{name, int64(v.FileSize), "video/mp4"}
	case msg.Audio != nil:
		au := msg.Audio
		name := au.FileName
		if name == "" {
			name = au.Performer + " - " + au.Title + ".mp3"
		}
		fi = fInfo{name, int64(au.FileSize), "audio/mpeg"}
	case msg.Voice != nil:
		v := msg.Voice
		fi = fInfo{"voice_" + v.FileUniqueID[:8] + ".ogg", int64(v.FileSize), "audio/ogg"}
	case msg.VideoNote != nil:
		vn := msg.VideoNote
		fi = fInfo{"videonote_" + vn.FileUniqueID[:8] + ".mp4", int64(vn.FileSize), "video/mp4"}
	case len(msg.Photo) > 0:
		ph := msg.Photo[len(msg.Photo)-1]
		fi = fInfo{"photo_" + ph.FileUniqueID[:8] + ".jpg", int64(ph.FileSize), "image/jpeg"}
	default:
		return fInfo{}, false
	}
	if fi.mimeType == "" {
		fi.mimeType = "application/octet-stream"
	}
	return fi, true
}

func (a *App) onFile(ctx context.Context, msg *tgbotapi.Message) {
	if msg.From.ID == a.cfg.AdminID && a.cache.isAdvertisePending(ctx, msg.From.ID) &&
		(msg.Video != nil || len(msg.Photo) > 0) {
		a.cache.clearAdvertisePending(ctx, msg.From.ID)
		streamURL, adType, err := a.storeAdvertiseMedia(ctx, msg)
		if err != nil {
			a.logger.Error("advertise media store failed", zap.Error(err))
			a.pool.send(msg.Chat.ID, "❌ Advertise media store nahi ho paya, dobara try karo.")
			return
		}
		if err := a.cache.setAdvertise(ctx, &adSettings{Enabled: true, Type: adType, URL: streamURL}); err != nil {
			a.pool.send(msg.Chat.ID, "❌ Advertise set nahi ho paya, dobara try karo.")
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"✅ Advertise banner set ho gaya \\(%s\\)\\.\n\nWatch page ke top par ab yeh highlight hoke dikhega\\.", adType))
		return
	}

	if groupID, active := a.cache.getMultiUploadSession(ctx, msg.From.ID); active {
		a.onMultiUploadFile(ctx, msg, groupID)
		return
	}

	fi, ok := extractFileInfo(msg)
	if !ok {
		a.pool.send(msg.Chat.ID, "⚠️ Unsupported file type.")
		return
	}

	procMsg, _ := a.pool.primary().Send(tgbotapi.NewMessage(msg.Chat.ID, "⏳ Processing..."))

	fwdMsg, err := a.pool.next().Send(tgbotapi.NewForward(a.cfg.DBChannelID, msg.Chat.ID, msg.MessageID))
	if err != nil {
		a.logger.Error("forward failed", zap.Error(err))
		if procMsg.MessageID != 0 {
			a.pool.delMsg(msg.Chat.ID, procMsg.MessageID)
		}
		a.pool.send(msg.Chat.ID, "❌ Failed to store file. Try again.")
		return
	}

	hash := makeShortHash(fi.fileName, fi.fileSize, fwdMsg.MessageID)
	slug := uuid.New().String()
	channelID := toInternalChannelID(a.cfg.DBChannelID)

	rec := &FileRecord{
		ID:           slug,
		MessageID:    fwdMsg.MessageID,
		ChannelID:    channelID,
		FileName:     fi.fileName,
		FileSize:     fi.fileSize,
		MimeType:     fi.mimeType,
		Hash:         hash,
		UploaderID:   msg.From.ID,
		UploaderName: msg.From.UserName,
	}

	if err := a.db.saveFile(ctx, rec); err != nil {
		a.logger.Error("db save failed", zap.Error(err))
		if procMsg.MessageID != 0 {
			a.pool.delMsg(msg.Chat.ID, procMsg.MessageID)
		}
		a.pool.send(msg.Chat.ID, "❌ Database error. Try again.")
		return
	}

	a.cache.setFile(ctx, slug, &cachedFile{
		MessageID: fwdMsg.MessageID,
		ChannelID: channelID,
		FileName:  fi.fileName,
		FileSize:  fi.fileSize,
		MimeType:  fi.mimeType,
		Hash:      hash,
	})

	if procMsg.MessageID != 0 {
		a.pool.delMsg(msg.Chat.ID, procMsg.MessageID)
	}

	base := a.cfg.baseURL()
	streamLink := fmt.Sprintf("%s/stream/%s", base, slug)
	dlLink := fmt.Sprintf("%s/dl/%s", base, slug)
	watchLink := fmt.Sprintf("%s/watch/%s", base, slug)

	text := fmt.Sprintf(
		"✅ *File Stored\\!*\n\n📄 `%s`\n📦 `%s`\n🆔 `%s`\n\n▶️ [Stream](%s)\n⬇️ [Download](%s)\n📺 [Watch Online](%s)\n\n🔒 Permanent link\\!\n\n_Tap the ID above to copy it — use it with /setpass, /expire, /delete_",
		mdEscape(fi.fileName), formatSize(fi.fileSize), slug, streamLink, dlLink, watchLink,
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("▶️ Stream", streamLink),
			tgbotapi.NewInlineKeyboardButtonURL("⬇️ Download", dlLink),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("📺 Watch Online", watchLink),
		),
	)
	a.pool.sendKB(msg.Chat.ID, text, kb)

	if a.cfg.LogChannelID != 0 {
		a.pool.sendMD(a.cfg.LogChannelID, fmt.Sprintf(
			"📁 *New File*\n👤 [%s](tg://user?id=%d)\n📄 `%s`\n📦 `%s`\n🔗 %s",
			mdEscape(msg.From.FirstName), msg.From.ID,
			mdEscape(fi.fileName), formatSize(fi.fileSize), streamLink,
		))
	}
}

// onMultiUploadFile handles a file received while a /u ... /d multi-quality
// session is active. Each file is stored under the same GroupID and tagged
// with whatever quality label its filename reveals (e.g. "720p"). No public
// link is sent per-file — that happens once for the whole group on /d.
func (a *App) onMultiUploadFile(ctx context.Context, msg *tgbotapi.Message, groupID string) {
	fi, ok := extractFileInfo(msg)
	if !ok {
		a.pool.send(msg.Chat.ID, "⚠️ Unsupported file type — skip kar diya. Baaki files bhejte raho ya /d se finish karo.")
		return
	}

	fwdMsg, err := a.pool.next().Send(tgbotapi.NewForward(a.cfg.DBChannelID, msg.Chat.ID, msg.MessageID))
	if err != nil {
		a.logger.Error("forward failed (multi-upload)", zap.Error(err))
		a.pool.send(msg.Chat.ID, "❌ Failed to store file. Try again.")
		return
	}

	label, rank := detectQuality(fi.fileName)
	hash := makeShortHash(fi.fileName, fi.fileSize, fwdMsg.MessageID)
	slug := uuid.New().String()
	channelID := toInternalChannelID(a.cfg.DBChannelID)
	gid := groupID

	rec := &FileRecord{
		ID:           slug,
		MessageID:    fwdMsg.MessageID,
		ChannelID:    channelID,
		FileName:     fi.fileName,
		FileSize:     fi.fileSize,
		MimeType:     fi.mimeType,
		Hash:         hash,
		UploaderID:   msg.From.ID,
		UploaderName: msg.From.UserName,
		GroupID:      &gid,
		QualityLabel: label,
		QualityRank:  rank,
	}

	if err := a.db.saveFile(ctx, rec); err != nil {
		a.logger.Error("db save failed (multi-upload)", zap.Error(err))
		a.pool.send(msg.Chat.ID, "❌ Database error. Try again.")
		return
	}

	a.cache.setFile(ctx, slug, &cachedFile{
		MessageID: fwdMsg.MessageID, ChannelID: channelID,
		FileName: fi.fileName, FileSize: fi.fileSize, MimeType: fi.mimeType, Hash: hash,
		GroupID: &gid, QualityLabel: label, QualityRank: rank,
	})
	a.cache.addMultiUploadEntry(ctx, groupID, fmt.Sprintf("%s|%s", label, fi.fileName))

	labelDisp := label
	if labelDisp == "" {
		labelDisp = "⚠️ quality naam se pehchaani nahi gayi (filename mein 480p/720p/1080p jaisa kuch likho)"
	}
	a.pool.send(msg.Chat.ID, fmt.Sprintf(
		"✅ Added: %s\nQuality: %s\n\nAur file bhejo, ya /d bhej ke finish karo.",
		fi.fileName, labelDisp,
	))
}

func (a *App) onVisitorActionCallback(ctx context.Context, cb *tgbotapi.CallbackQuery) {
	answer := func(text string) {
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, text))
	}
	if cb.From.ID != a.cfg.AdminID {
		answer("❌ Admin only.")
		return
	}
	parts := strings.SplitN(cb.Data, ":", 3)
	if len(parts) != 3 {
		answer("❌ Bad button.")
		return
	}
	action := parts[1]
	accessID, convErr := strconv.Atoi(parts[2])
	if convErr != nil {
		answer("❌ Bad button.")
		return
	}

	var found bool
	var err error
	var statusLine, toast string
	var newKB tgbotapi.InlineKeyboardMarkup
	switch action {
	case "appr":
		found, err = a.db.approveByID(ctx, accessID)
		statusLine, toast = "✅ *Approved*", "✅ Approved!"
		newKB = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚫 Block", fmt.Sprintf("va:blk:%d", accessID)),
		))
	case "blk":
		found, err = a.db.blockByID(ctx, accessID, true)
		statusLine, toast = "🚫 *Blocked*", "🚫 Blocked!"
		newKB = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Unblock", fmt.Sprintf("va:unblk:%d", accessID)),
		))
	case "unblk":
		found, err = a.db.blockByID(ctx, accessID, false)
		statusLine, toast = "↩️ *Unblocked*", "↩️ Unblocked!"
		newKB = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Approve", fmt.Sprintf("va:appr:%d", accessID)),
			tgbotapi.NewInlineKeyboardButtonData("🚫 Block", fmt.Sprintf("va:blk:%d", accessID)),
		))
	default:
		answer("❌ Bad button.")
		return
	}
	if err != nil {
		answer("❌ Failed, dobara try karo.")
		return
	}
	if !found {
		answer("❌ Access ID nahi mila.")
		return
	}

	baseText := cb.Message.Text
	if idx := strings.Index(baseText, "\n\nStatus:"); idx != -1 {
		baseText = baseText[:idx]
	}
	edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID, baseText+"\n\nStatus: "+statusLine)
	edit.ParseMode = "MarkdownV2"
	a.pool.primary().Send(edit)
	a.pool.primary().Send(tgbotapi.NewEditMessageReplyMarkup(cb.Message.Chat.ID, cb.Message.MessageID, newKB))

	answer(toast)
}

func (a *App) onCallback(ctx context.Context, cb *tgbotapi.CallbackQuery) {
	if strings.HasPrefix(cb.Data, "va:") {
		a.onVisitorActionCallback(ctx, cb)
		return
	}
	if cb.Data == "confirm_delete_all" {
		if cb.From.ID != a.cfg.AdminID {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Admin only."))
			return
		}
		n, err := a.db.deleteAllFiles(ctx)
		a.cache.clearAllFileCache(ctx)
		if err != nil {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Failed."))
			edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Delete failed — try again.")
			a.pool.primary().Send(edit)
			return
		}
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "✅ Deleted!"))
		edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID,
			fmt.Sprintf("🗑️ %d files permanently deleted. All links are now dead.", n))
		a.pool.primary().Send(edit)
		return
	}
	if cb.Data == "cancel_delete_all" {
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "Cancelled."))
		edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Cancelled — koi file delete nahi hui.")
		a.pool.primary().Send(edit)
		return
	}
	if cb.Data == "verify_fsub" {
		a.cache.delFsub(ctx, cb.From.ID)
		ok, _ := a.pool.isMember(a.cfg.MainChannelID, cb.From.ID)
		a.cache.setFsub(ctx, cb.From.ID, ok)
		if !ok {
			ans := tgbotapi.NewCallback(cb.ID, "❌ Not joined yet!")
			a.pool.primary().Request(ans)
			return
		}
		ans := tgbotapi.NewCallback(cb.ID, "✅ Verified!")
		a.pool.primary().Request(ans)
		edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID,
			"✅ *Access granted\\!* Send me a file now\\.")
		edit.ParseMode = "MarkdownV2"
		a.pool.primary().Send(edit)
		return
	}
	a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, ""))
}

func (a *App) sendFsubPrompt(chatID int64) {
	chID := a.cfg.MainChannelID
	if chID < 0 {
		chID = -chID - 1000000000000
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("📢 Join Channel",
				fmt.Sprintf("https://t.me/c/%d", chID)),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ I Joined — Verify", "verify_fsub"),
		),
	)
	a.pool.sendKB(chatID, "🔒 *Join Required*\n\nJoin our channel first, then press Verify\\.", kb)
}

// ============================================================
// HTTP STREAMING
// ============================================================

func (a *App) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges, Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	path := r.URL.Path
	switch {
	case path == "/" || path == "":
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html><body style="background:#1b1c26;color:#7aa2f7;font-family:monospace;text-align:center;padding:50px">
<h1>📚 Astratoonix Education Platform</h1>
<p style="color:#8b949e">Science &middot; Math &middot; Physics &middot; Chemistry &middot; English &middot; Coding (Java, C, Python, Go &amp; more)</p>
<p style="color:#484f58;font-size:12px">A self-hosted learning platform for students, built by Raj Dev.</p>
</body></html>`)

	case path == "/health" || path == "/ping":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","mtproto":%t}`, a.mtPool.isAnyReady())

	case strings.HasPrefix(path, "/stream/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/stream/"), "/")
		a.handleStream(w, r, slug, false)

	case strings.HasPrefix(path, "/dl/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/dl/"), "/")
		a.handleStream(w, r, slug, true)

	case strings.HasPrefix(path, "/watch/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/watch/"), "/")
		a.handleWatch(w, r, slug)

	case strings.HasPrefix(path, "/heartbeat/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/heartbeat/"), "/")
		a.handleHeartbeat(w, r, slug)

	case strings.HasPrefix(path, "/notify/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/notify/"), "/")
		a.handleNotify(w, r, slug)

	case strings.HasPrefix(path, "/approval-status/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/approval-status/"), "/")
		a.handleApprovalStatus(w, r, slug)

	case strings.HasPrefix(path, "/livecount/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/livecount/"), "/")
		a.handleLiveCount(w, r, slug)

	case path == "/admin":
		a.handleAdmin(w, r)

	default:
		http.NotFound(w, r)
	}
}

func (a *App) handleHeartbeat(w http.ResponseWriter, r *http.Request, slug string) {
	if slug == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	deviceID := getOrSetDeviceID(w, r)
	a.cache.heartbeat(r.Context(), slug, deviceID)
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"ok":true}`)
}

func (a *App) handleLiveCount(w http.ResponseWriter, r *http.Request, slug string) {
	if slug == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	n := a.cache.liveCount(r.Context(), slug)
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"live":%d}`, n)
}

func (a *App) handleStream(w http.ResponseWriter, r *http.Request, slug string, download bool) {
	ctx := r.Context()

	if slug == "" {
		http.Error(w, "Missing file ID", http.StatusBadRequest)
		return
	}

	var messageID int
	var channelID int64
	var fileName, mimeType string
	var fileSize int64
	var expiresAt *time.Time
	var passwordHash *string

	cached := a.cache.getFile(ctx, slug)
	if cached != nil {
		messageID = cached.MessageID
		channelID = cached.ChannelID
		fileName = cached.FileName
		fileSize = cached.FileSize
		mimeType = cached.MimeType
		expiresAt = cached.ExpiresAt
		passwordHash = cached.PasswordHash
	} else {
		rec, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			http.Error(w, "File not found.", http.StatusNotFound)
			return
		}
		messageID = rec.MessageID
		channelID = rec.ChannelID
		fileName = rec.FileName
		fileSize = rec.FileSize
		mimeType = rec.MimeType
		expiresAt = rec.ExpiresAt
		passwordHash = rec.PasswordHash
		a.cache.setFile(ctx, slug, &cachedFile{
			MessageID: messageID, ChannelID: channelID,
			FileName: fileName, FileSize: fileSize, MimeType: mimeType,
			ExpiresAt: expiresAt, PasswordHash: passwordHash,
			GroupID: rec.GroupID, QualityLabel: rec.QualityLabel, QualityRank: rec.QualityRank,
		})
	}

	if isExpired(expiresAt) {
		http.Error(w, "⏳ This link has expired.", http.StatusGone)
		return
	}

	if passwordHash != nil && *passwordHash != "" {
		if !hasValidPasswordCookie(r, slug, *passwordHash) {
			http.Error(w, "🔒 This link is password protected. Open the /watch page and enter the password first.", http.StatusUnauthorized)
			return
		}
		setPasswordCookie(w, slug, *passwordHash)
	}

	deviceID := getOrSetDeviceID(w, r)
	approval, isNewDevice, aErr := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if aErr != nil {
		a.logger.Warn("approval lookup failed (stream)", zap.Error(aErr))
	}
	if approval != nil && approval.Blocked {
		http.Error(w, "🚫 Access blocked.", http.StatusForbidden)
		return
	}
	if approval == nil || !approval.Approved {
		accessID := 0
		visitorName := ""
		if approval != nil {
			accessID = approval.AccessID
			visitorName = approval.VisitorName
		}
		a.notifyNewAccessID(isNewDevice, accessID, fileName, visitorName)
		http.Error(w, fmt.Sprintf(
			"🔒 Not approved yet. Open the /watch page for this link — your Access ID is %05d. Ask admin to run /approve %05d.",
			accessID, accessID), http.StatusUnauthorized)
		return
	}

	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	if !a.mtPool.isAnyReady() {
		slog.Warn("MTProto not ready yet, waiting...")
		for i := 0; i < 10; i++ {
			time.Sleep(1 * time.Second)
			if a.mtPool.isAnyReady() {
				break
			}
		}
		if !a.mtPool.isAnyReady() {
			http.Error(w, "Service starting up, please retry in a moment.", http.StatusServiceUnavailable)
			return
		}
	}

	location, tgSize, err := a.mtPool.getFileLocation(ctx, channelID, messageID)
	if err != nil {
		a.logger.Error("getFileLocation failed",
			zap.Int("message_id", messageID),
			zap.Int64("channel_id", channelID),
			zap.Error(err),
		)
		http.Error(w, "Cannot retrieve file from Telegram.", http.StatusBadGateway)
		return
	}

	if fileSize <= 0 && tgSize > 0 {
		fileSize = tgSize
	}

	start, end := int64(0), fileSize-1
	isRange := false
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" && fileSize > 0 {
		rng := strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.SplitN(rng, "-", 2)
		if len(parts) == 2 {
			s := strings.TrimSpace(parts[0])
			e := strings.TrimSpace(parts[1])
			if s != "" {
				start, _ = strconv.ParseInt(s, 10, 64)
			}
			if e != "" {
				end, _ = strconv.ParseInt(e, 10, 64)
			}
			if end >= fileSize {
				end = fileSize - 1
			}
			if start >= 0 && start <= end {
				isRange = true
			}
		}
	}

	disposition := "inline"
	if download {
		disposition = "attachment"
	}
	w.Header().Set("Content-Type", mimeType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`%s; filename="%s"`, disposition, sanitizeName(fileName)))

	contentLength := end - start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))

	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if r.Method == http.MethodHead {
		return
	}

	bot := a.mtPool.next()
	bot.mu.Lock()
	api := bot.api
	bot.mu.Unlock()

	reader := newTgFileReader(ctx, api, a.cfg, location, fileSize, start, end)
	defer reader.Close()
	if _, err := io.CopyN(w, reader, contentLength); err != nil {
		a.logger.Debug("stream ended", zap.Error(err))
	}
}

type WatchData struct {
	FileName    string
	FileSize    string
	MimeType    string
	StreamURL   string
	DownloadURL string
	WatchURL    string
	ViewCount   string
	LiveCount   string
	DeviceID    string
	Slug        string

	AdEnabled bool
	AdType    string
	AdURL     string

	QualitiesJSON template.JS
}

// qualityOption is one entry in the watch page's quality switcher dropdown.
type qualityOption struct {
	Label  string `json:"label"`
	URL    string `json:"url"`
	Active bool   `json:"active"`
}

func renderSplash(w http.ResponseWriter, aboutText, nextURL string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Astratoonix</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body {
  background:#060810; color:#e7ecf7; font-family:'Inter',system-ui,sans-serif;
  min-height:100vh; display:flex; flex-direction:column; align-items:center; justify-content:center;
  overflow:hidden; text-align:center; padding:20px;
}
.brand {
  font-family:'JetBrains Mono',monospace; font-weight:800; font-size:clamp(32px,9vw,58px);
  letter-spacing:3px; background:linear-gradient(90deg,#38d6ff,#7c8fff,#ffd166,#38d6ff);
  background-size:300%% auto; -webkit-background-clip:text; background-clip:text; color:transparent;
  animation:shimmer 3s linear infinite, popIn .8s cubic-bezier(.2,.9,.3,1.2);
}
@keyframes shimmer { to { background-position:300%% center; } }
@keyframes popIn { from { opacity:0; transform:scale(.85); } to { opacity:1; transform:scale(1); } }
.tagline {
  margin-top:12px; font-size:clamp(12px,2.6vw,15px); color:#8ea0d0; font-weight:500;
  opacity:0; animation:fadeIn 1s ease .6s forwards;
}
@keyframes fadeIn { to { opacity:1; } }
.edu-badge {
  margin-top:18px; display:inline-flex; align-items:center; gap:8px; padding:7px 16px;
  border-radius:20px; background:rgba(56,214,255,.1); border:1px solid rgba(56,214,255,.25);
  font-size:12px; color:#38d6ff; font-weight:600; opacity:0; animation:fadeIn 1s ease 1s forwards;
}
.ticker-wrap {
  width:100%%; max-width:640px; margin-top:36px; padding:12px 0; background:#0b0f1a;
  border-top:1px solid #182034; border-bottom:1px solid #182034;
  opacity:0; animation:fadeIn 1s ease 1.4s forwards;
}
.ticker-wrap marquee { font-size:12.5px; color:#6b7bab; }
.progress-wrap { width:min(240px,70vw); height:3px; border-radius:3px; background:#141a29; margin-top:30px; overflow:hidden; }
.progress-bar { height:100%%; width:0%%; background:linear-gradient(90deg,#38d6ff,#ffd166); animation:fill 10s linear forwards; }
@keyframes fill { to { width:100%%; } }
.skip-hint { margin-top:14px; font-size:11px; color:#3d4864; }
</style></head>
<body>
  <div class="brand">ASTRATOONIX</div>
  <div class="tagline">Empowering Developers Through Authentic Learning</div>
  <div class="edu-badge"><i class="fas fa-graduation-cap"></i>&nbsp; Education Platform — Original Content Only</div>
  <div class="ticker-wrap"><marquee behavior="scroll" direction="left" scrollamount="4">%s</marquee></div>
  <div class="progress-wrap"><div class="progress-bar"></div></div>
  <div class="skip-hint">Starting in a few seconds…</div>
<script>
setTimeout(function() {
  window.location.href = %q;
}, 10000);
</script>
</body></html>`, html.EscapeString(aboutText), nextURL)
}

func astratoonixSubjectTiles() string {
	subjects := []string{
		"MATH", "PHYSICS", "CHEMISTRY", "BIOLOGY", "SCIENCE", "ENGLISH",
		"PYTHON", "C++", "JAVASCRIPT", "CSS", "LINUX", "HTML",
		"HISTORY", "GEOGRAPHY", "ALGEBRA", "GIT", "SQL", "DSA",
	}
	var b strings.Builder
	for _, s := range subjects {
		fmt.Fprintf(&b, `<span class="stile">%s</span>`, html.EscapeString(s))
	}
	return b.String()
}

func astratoonixGateCSS() string {
	return `
* { margin:0; padding:0; box-sizing:border-box; }
html,body { height:100%; }
body {
  background:#060810; color:#e7ecf7; font-family:'Inter',system-ui,sans-serif;
  min-height:100vh; overflow-x:hidden;
}
.stage {
  position:relative; min-height:100vh; width:100%;
  display:flex; align-items:center; justify-content:center; padding:22px 14px;
  border:3px solid #17324a; box-sizing:border-box;
}
.stage::before {
  content:''; position:fixed; inset:0; z-index:0; pointer-events:none;
  background:
    radial-gradient(circle at 15% 20%, rgba(56,214,255,.10) 0%, transparent 45%),
    radial-gradient(circle at 85% 80%, rgba(155,92,255,.10) 0%, transparent 45%);
}
.subject-grid {
  position:fixed; inset:0; z-index:0; pointer-events:none;
  display:flex; flex-wrap:wrap; align-content:center; justify-content:center;
  gap:26px 34px; padding:40px; opacity:0.05; overflow:hidden;
}
.stile {
  font-family:'JetBrains Mono',monospace; font-weight:800; font-size:26px;
  letter-spacing:2px; color:#38d6ff; white-space:nowrap;
}
#boot {
  position:fixed; inset:0; z-index:999; background:#060810;
  display:flex; align-items:center; justify-content:center; flex-direction:column; gap:8px;
  font-family:'JetBrains Mono',monospace;
  opacity:1; visibility:visible;
  transition:opacity .6s ease, visibility .6s ease;
}
#boot.hide { opacity:0; visibility:hidden; pointer-events:none; }
#boot .brand {
  font-size:clamp(26px,7vw,44px); font-weight:800; letter-spacing:2px; color:#38d6ff;
  border-right:3px solid #38d6ff; white-space:nowrap; overflow:hidden; width:0;
}
#boot.play .brand { animation: typeBrand 2.2s steps(11,end) forwards, caretBlink .8s step-end infinite; }
#boot.instant .brand { width:calc(11ch + 24px); border-right-color:transparent; }
#boot .edu-tag {
  font-size:12.5px; letter-spacing:3px; font-weight:700; color:#7ee787; opacity:0; margin-top:2px;
}
#boot.play .edu-tag { animation: fadeIn .6s ease 2.5s forwards; }
#boot.instant .edu-tag { opacity:1; }
#boot .by { font-size:14px; letter-spacing:4px; color:#6b7bab; opacity:0; margin-top:4px; }
#boot.play .by { animation: fadeIn .5s ease 3.2s forwards; }
#boot.instant .by { opacity:1; }
#boot .by b { color:#9d7cf7; }
@keyframes typeBrand { from{width:0} to{width:calc(11ch + 24px)} }
@keyframes caretBlink { 50%{border-color:transparent} }
@keyframes fadeIn { to{opacity:1} }

.card {
  position:relative; z-index:1; width:min(400px,100%);
  background:linear-gradient(180deg,#0b0f1a,#0a0d16);
  border:1px solid #182034; border-radius:18px; padding:26px 24px 22px;
  box-shadow:0 20px 60px rgba(0,0,0,.55), 0 0 0 1px rgba(56,214,255,.08);
  opacity:0; transform:translateY(10px);
  transition:opacity .6s ease, transform .6s ease;
}
.card.show { opacity:1; transform:translateY(0); }

.badges { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:16px; }
.badge {
  font-size:10.5px; font-weight:700; letter-spacing:.4px; padding:5px 9px; border-radius:20px;
  background:#0f1a2a; border:1px solid #1d3350; color:#6fe3ff;
  display:inline-flex; align-items:center; gap:5px;
}
.badge.free { color:#7dffb0; border-color:#1f4632; background:#0d1c15; }
.badge.https { color:#ffd166; border-color:#4a3a12; background:#1c1608; }

.ct-btn {
  display:flex; align-items:center; gap:10px; text-decoration:none; color:#fff;
  padding:11px 14px; border-radius:12px; margin-bottom:10px; font-size:13.5px; font-weight:600;
  cursor:pointer; border:none; width:100%; font-family:'Inter',sans-serif;
}
.ct-btn:last-child { margin-bottom:0; }
.ct-btn.tg { background:#0088cc; }
.ct-btn.ig { background:linear-gradient(45deg,#f09433,#e6683c,#dc2743,#cc2366,#bc1888); }
.ct-btn.notify {
  background:linear-gradient(90deg,#ff2d6a,#ff9a2d,#e6ff2d,#2dff7a,#2dd4ff,#7a2dff,#ff2d6a);
  background-size:300% 100%;
  animation:rgbFlow 5s linear infinite;
}
@keyframes rgbFlow { 0% { background-position:0% 50%; } 100% { background-position:300% 50%; } }
.ct-btn i.brand { font-size:16px; flex-shrink:0; }
.ct-btn span.label { flex:1; text-align:left; }

.stack-line {
  margin-top:16px; padding-top:14px; border-top:1px solid #131a2a;
  display:flex; flex-wrap:wrap; gap:6px; justify-content:center;
}
.stack-chip {
  font-size:10.5px; font-weight:700; color:#8ea3cf; background:#0d1322;
  border:1px solid #1a2436; border-radius:6px; padding:4px 8px; font-family:'JetBrains Mono',monospace;
}

@media (prefers-reduced-motion: reduce) {
  #boot { transition:none !important; }
  #boot .brand, #boot .edu-tag, #boot .by { animation:none !important; opacity:1 !important; width:11ch !important; border-right-color:transparent !important; }
  .card { transition:none !important; }
}
`
}

func astratoonixBootHTML(subjectTilesHTML string) string {
	return fmt.Sprintf(`
<div id="boot">
  <div class="brand">ASTRATOONIX</div>
  <div class="edu-tag">🎓 EDUCATIONAL PLATFORM</div>
  <div class="by">BUILT BY <b>RAJ</b></div>
</div>
<div class="subject-grid">%s</div>`, subjectTilesHTML)
}

func astratoonixBootJS(playBoot bool) string {
	return fmt.Sprintf(`
(function(){
  var boot = document.getElementById('boot');
  var card = document.querySelector('.card');
  function reveal(){
    boot.classList.add('hide');
    if (card) card.classList.add('show');
    var vid = document.getElementById('ppv');
    if (vid) { vid.play().catch(function(){}); }
  }
  if (%t) {
    boot.classList.add('play');
    setTimeout(reveal, 9400);
  } else {
    boot.classList.add('instant');
    reveal();
  }
})();`, playBoot)
}

func renderPasswordPrompt(w http.ResponseWriter, slug string, wrong bool, accessID int, prefillName string, videoURL string, promptImages []string, contactTelegram, contactInstagram string) {
	msg := ""
	if wrong {
		msg = `<div style="color:#ff6b7a;margin:0 0 14px;font-size:13.5px;font-weight:600;
background:rgba(255,107,122,.1);border:1px solid rgba(255,107,122,.3);border-radius:8px;padding:10px 12px;">
❌ Galat password, dobara try karo.</div>`
	}

	videoBlock := ""
	if videoURL != "" {
		videoBlock = fmt.Sprintf(`
<div style="position:relative;margin:14px 0 16px;border-radius:12px;overflow:hidden;
box-shadow:0 0 0 1px rgba(56,214,255,.25),0 8px 24px rgba(0,0,0,.5);">
<video id="ppv" src="%s" autoplay muted loop playsinline
style="width:100%%;display:block;background:#000;"></video>
<button type="button" onclick="var v=document.getElementById('ppv');v.muted=!v.muted;this.textContent=v.muted?'🔇 Tap for sound':'🔊 Sound on';"
style="position:absolute;bottom:10px;right:10px;background:rgba(6,8,14,.75);color:#fff;
border:1px solid rgba(255,255,255,.25);backdrop-filter:blur(4px);
border-radius:20px;padding:6px 14px;font-size:12.5px;font-weight:600;cursor:pointer;">🔇 Tap for sound</button>
</div>`,
			html.EscapeString(videoURL))
	}

	imageBlock := ""
	if len(promptImages) > 0 {
		var urls strings.Builder
		for i, u := range promptImages {
			if i > 0 {
				urls.WriteString(",")
			}
			urls.WriteString(fmt.Sprintf("%q", u))
		}
		imageBlock = fmt.Sprintf(`
<div style="margin-top:16px;">
<img id="ppimg" src="%s" style="width:100%%;border-radius:10px;display:block;
box-shadow:0 4px 16px rgba(0,0,0,.4);transition:opacity .4s ease;">
</div>
<script>
(function(){
var imgs=[%s];
var el=document.getElementById('ppimg');
if(!el||imgs.length<2)return;
setInterval(function(){
var next=imgs[Math.floor(Math.random()*imgs.length)];
el.style.opacity=0;
setTimeout(function(){el.src=next;el.style.opacity=1;},400);
},4000);
})();
</script>`, html.EscapeString(promptImages[0]), urls.String())
	}

	contactBlock := ""
	if contactTelegram != "" || contactInstagram != "" {
		var links strings.Builder
		if contactTelegram != "" {
			fmt.Fprintf(&links, `
<a href="https://t.me/%s" target="_blank" rel="noopener" class="ct-btn tg">
<i class="fa-brands fa-telegram brand"></i><span class="label">@%s</span></a>`,
				url.QueryEscape(contactTelegram), html.EscapeString(contactTelegram))
		}
		if contactInstagram != "" {
			fmt.Fprintf(&links, `
<a href="https://instagram.com/%s" target="_blank" rel="noopener" class="ct-btn ig">
<i class="fa-brands fa-instagram brand"></i><span class="label">@%s</span></a>`,
				url.QueryEscape(contactInstagram), html.EscapeString(contactInstagram))
		}
		contactBlock = fmt.Sprintf(`
<div style="margin-top:16px;padding-top:14px;border-top:1px solid #1c2130;">
%s
</div>`, links.String())
	}

	subjectTilesHTML := astratoonixSubjectTiles()
	accessIDBlock := fmt.Sprintf(`
<div style="margin:0 0 16px;padding:12px;border-radius:10px;background:#0d1322;border:1px solid #1a2436;text-align:center;">
  <div style="font-size:10.5px;letter-spacing:.5px;color:#6b7bab;margin-bottom:4px;">YOUR ACCESS ID — send this to admin for approval</div>
  <div style="font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:800;color:#38d6ff;letter-spacing:3px;">%05d</div>
</div>`, accessID)

	devContact := contactTelegram
	if devContact == "" {
		devContact = "raj_dev_01"
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>🔒 Password Required — Astratoonix</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
%s
.lock-icon { font-size:30px; margin-bottom:6px; }
h2.title { font-size:17px; font-weight:700; margin-bottom:4px; }
.subtitle { font-size:12px; color:#6b7bab; margin-bottom:16px; line-height:1.5; }
.subtitle b { color:#9db3d9; }

input[type=password], input[type=text] {
  width:100%%; box-sizing:border-box; padding:13px 14px; border-radius:10px;
  border:1px solid #1c2537; background:#060810; color:#fff; font-size:15px;
  margin-bottom:12px; font-family:'Inter',sans-serif;
  transition:box-shadow .2s ease, color .2s ease;
}
input[type=password]:focus, input[type=text]:focus { outline:none; border-color:#38d6ff; box-shadow:0 0 6px rgba(56,214,255,.65),0 0 16px rgba(157,124,247,.35); }
input[type=password]:not(:placeholder-shown), input[type=text]:not(:placeholder-shown) { color:#7cf3ff; text-shadow:0 0 6px rgba(56,214,255,.7); }
button.unlock {
  width:100%%; padding:13px; border:none; border-radius:10px;
  background:linear-gradient(135deg,#38d6ff,#9d7cf7); color:#060810;
  font-weight:800; font-size:15px; cursor:pointer; letter-spacing:.3px;
}
button.unlock:active { transform:scale(.98); }

.collab {
  position:relative; z-index:1; width:min(400px,100%%); margin-top:16px;
  background:linear-gradient(135deg,#1c1608,#0b0f1a); border:1px solid #4a3a12;
  border-radius:14px; padding:16px 18px; text-align:center;
}
.collab .h { font-size:13px; font-weight:800; color:#ffd166; margin-bottom:5px; }
.collab .b { font-size:12px; color:#c9b98f; line-height:1.6; margin-bottom:10px; }
.collab a.cta {
  display:inline-flex; align-items:center; gap:6px; text-decoration:none;
  background:#ffd166; color:#1c1608; font-weight:800; font-size:12.5px;
  padding:8px 16px; border-radius:20px;
}
</style></head>
<body>
%s
<div class="stage">
  <div style="display:flex;flex-direction:column;align-items:center;width:100%%;">
    <div class="card">
      <div class="badges">
        <span class="badge">🔒 PRIVATE</span>
        <span class="badge https">🔐 HTTPS</span>
        <span class="badge free">💯 FREE</span>
      </div>
      <div class="lock-icon">🔒</div>
      <h2 class="title">Password Protected Link</h2>
      <div class="subtitle">Ek <b>education platform</b> — Science, Math aur coding (Python, C++, Linux, JS, CSS) sab kuchh yahin milega.</div>
      %s
      %s
      %s
      <form method="POST" action="/watch/%s">
        <input type="text" name="name" placeholder="Your name" value="%s" autocomplete="off" autofocus required>
        <input type="password" name="pw" placeholder="Enter password" required>
        <button type="submit" class="unlock">Unlock</button>
      </form>
      %s
      <div class="stack-line">
        <span class="stack-chip">PYTHON</span>
        <span class="stack-chip">C++</span>
        <span class="stack-chip">LINUX</span>
        <span class="stack-chip">JS</span>
        <span class="stack-chip">CSS</span>
      </div>
      %s
    </div>

    <div class="collab">
      <div class="h">🤝 Devs wanted — earn commission</div>
      <div class="b">C++ ya web scraping aata hai? Website banane mein help karo — achcha commission milega, plus poora complete course (in English) free.</div>
      <a class="cta" href="https://t.me/%s" target="_blank">✈️ DM @%s</a>
    </div>
  </div>
</div>
<script>%s</script>
</body></html>`,
		astratoonixGateCSS(),
		astratoonixBootHTML(subjectTilesHTML),
		videoBlock, msg, accessIDBlock, slug, html.EscapeString(prefillName), imageBlock, contactBlock,
		url.QueryEscape(devContact), html.EscapeString(devContact),
		astratoonixBootJS(!wrong),
	)
}

func renderPendingApproval(w http.ResponseWriter, slug string, accessID int, prefillName, contactTelegram, contactInstagram string) {
	tgUser := contactTelegram
	if tgUser == "" {
		tgUser = "raj_dev_01"
	}
	igUser := contactInstagram
	if igUser == "" {
		igUser = "raj_dev_01"
	}
	subjectTilesHTML := astratoonixSubjectTiles()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Waiting for Approval — Astratoonix</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
%s
.spin {
  width:32px; height:32px; margin:0 auto 14px; border-radius:50%%;
  border:3px solid rgba(255,209,102,.2); border-top-color:#ffd166;
  animation:spin .9s linear infinite;
}
@keyframes spin { to { transform:rotate(360deg); } }
h2 { font-size:17px; font-weight:700; margin-bottom:4px; }
p { font-size:12px; color:#6b7bab; line-height:1.6; margin-bottom:14px; }
.edu-note {
  font-size:11px; color:#7ee787; background:rgba(126,231,135,.08);
  border:1px solid rgba(126,231,135,.2); border-radius:8px; padding:9px 11px;
  margin-bottom:16px; line-height:1.5; text-align:left;
}
.idbox {
  padding:14px; border-radius:10px; background:#0d1322; border:1px solid #1a2436; margin-bottom:18px;
}
.idbox .l { font-size:10.5px; letter-spacing:.5px; color:#6b7bab; margin-bottom:4px; }
.idbox .n { font-family:'JetBrains Mono',monospace; font-size:26px; font-weight:800; color:#ffd166; letter-spacing:3px; }
.name-input {
  width:100%%; padding:12px 14px; border-radius:12px; border:1px solid #1a2436;
  background:#0d1322; color:#e7ecf7; font-size:13.5px; margin-bottom:10px; font-family:'Inter',sans-serif;
}
.name-input:focus { outline:none; border-color:#38d6ff; }
.notify-status { font-size:11.5px; min-height:14px; margin:-2px 0 16px; color:#7ee787; }
.notify-status.err { color:#ff7b86; }
.ct-title { font-size:11.5px; color:#6b7bab; margin-bottom:10px; }
.ct-btn i.arrow { opacity:.85; }
.lang-switch { display:flex; justify-content:center; gap:6px; margin-top:16px; }
.lang-btn {
  font-size:11px; font-weight:700; padding:5px 11px; border-radius:20px; border:1px solid #1a2436;
  background:#0d1322; color:#6b7bab; cursor:pointer; font-family:'Inter',sans-serif;
}
.lang-btn.active { background:#38d6ff; color:#060810; border-color:transparent; }
</style></head>
<body>
%s
<div class="stage">
  <div style="display:flex;flex-direction:column;align-items:center;width:100%%;">
    <div class="card">
      <div class="badges">
        <span class="badge">🎓 EDUCATION</span>
        <span class="badge https">🔒 PRIVATE</span>
        <span class="badge free">✅ VERIFIED ACCESS</span>
      </div>
      <div class="spin"></div>
      <h2 data-i18n="h2">Password correct ✅</h2>
      <p data-i18n="p">Admin hasn't approved yet. Send this Access ID to admin — the page will unlock automatically once approved.</p>
      <div class="edu-note">🎓 Astratoonix ek education platform hai — Science, Math aur coding content ke liye. Content protect karne ke liye har naye device ko ek baar admin manually verify karta hai. Yeh spam nahi hai.</div>
      <div class="idbox">
        <div class="l" data-i18n="idLabel">YOUR ACCESS ID</div>
        <div class="n">%05d</div>
      </div>
      <input type="text" id="visitorNameInput" class="name-input" data-i18n-ph="namePh" placeholder="Your name" value="%s">
      <button type="button" class="ct-btn notify" id="notifyBtn">
        <i class="fas fa-paper-plane brand"></i>
        <span class="label" data-i18n="notifyLabel">Notify Admin</span>
      </button>
      <div class="notify-status" id="notifyStatus"></div>
      <div class="ct-title" data-i18n="ctTitle">Or message here directly</div>
      <a class="ct-btn tg" href="https://t.me/%s" target="_blank" rel="noopener">
        <i class="fa-brands fa-telegram brand"></i>
        <span class="label" data-i18n="tgLabel">Message on Telegram</span>
        <i class="fas fa-arrow-right arrow"></i>
      </a>
      <button type="button" class="ct-btn ig" onclick="messageOnInstagram()">
        <i class="fa-brands fa-instagram brand"></i>
        <span class="label" data-i18n="igLabel">Message on Instagram</span>
        <i class="fas fa-arrow-right arrow"></i>
      </button>
      <div class="lang-switch">
        <button class="lang-btn active" data-lang="en">EN</button>
        <button class="lang-btn" data-lang="hi">हिं</button>
        <button class="lang-btn" data-lang="bn">বাং</button>
      </div>
      <div class="stack-line">
        <span class="stack-chip">PYTHON</span>
        <span class="stack-chip">C++</span>
        <span class="stack-chip">LINUX</span>
        <span class="stack-chip">JS</span>
        <span class="stack-chip">CSS</span>
      </div>
    </div>
  </div>
</div>
<script>
var ACCESS_ID = "%05d";
var IG_USER = %q;
var SLUG = %q;
var NOTIFY_URL = "/notify/" + encodeURIComponent(SLUG);
var currentLang = 'en';
var I18N = {
  en: { h2:"Password correct ✅", p:"Admin hasn't approved yet. Send this Access ID to admin — the page will unlock automatically once approved.",
        idLabel:"YOUR ACCESS ID", namePh:"Your name", notifyLabel:"Notify Admin",
        ctTitle:"Or message here directly", tgLabel:"Message on Telegram", igLabel:"Message on Instagram",
        needName:"Please enter your name first.", sent:"✅ Sent! Admin will be notified.",
        wait:"⏳ Already sent — please wait a bit before sending again.", err:"❌ Something went wrong, try again." },
  hi: { h2:"Password sahi hai ✅", p:"Admin ne abhi tak approve nahi kiya. Yeh Access ID admin ko bhejo — approve hote hi yeh page apne aap khul jaayega.",
        idLabel:"AAPKA ACCESS ID", namePh:"Apna naam likho", notifyLabel:"Admin ko Notify Karo",
        ctTitle:"Ya seedha yahan message karo", tgLabel:"Telegram par message karo", igLabel:"Instagram par message karo",
        needName:"Pehle apna naam likho.", sent:"✅ Bhej diya! Admin ko notify ho gaya.",
        wait:"⏳ Pehle se bhej chuke ho — thoda ruk kar dobara try karo.", err:"❌ Kuch gadbad hui, dobara try karo." },
  bn: { h2:"পাসওয়ার্ড সঠিক ✅", p:"অ্যাডমিন এখনও অ্যাপ্রুভ করেননি। এই Access ID অ্যাডমিনকে পাঠান — অ্যাপ্রুভ হলেই পেজ নিজে থেকে খুলে যাবে।",
        idLabel:"আপনার অ্যাক্সেস আইডি", namePh:"আপনার নাম লিখুন", notifyLabel:"অ্যাডমিনকে নোটিফাই করুন",
        ctTitle:"অথবা এখানে সরাসরি মেসেজ করুন", tgLabel:"টেলিগ্রামে মেসেজ করুন", igLabel:"ইনস্টাগ্রামে মেসেজ করুন",
        needName:"আগে আপনার নাম লিখুন।", sent:"✅ পাঠানো হয়েছে! অ্যাডমিনকে জানানো হয়েছে।",
        wait:"⏳ আগেই পাঠানো হয়েছে — একটু অপেক্ষা করুন।", err:"❌ কিছু ভুল হয়েছে, আবার চেষ্টা করুন।" }
};
document.querySelectorAll('.lang-btn').forEach(function(btn){
  btn.addEventListener('click', function(){
    document.querySelectorAll('.lang-btn').forEach(function(b){ b.classList.remove('active'); });
    btn.classList.add('active');
    currentLang = btn.dataset.lang;
    var dict = I18N[currentLang];
    document.querySelectorAll('[data-i18n]').forEach(function(el){
      var key = el.getAttribute('data-i18n');
      if (dict[key]) el.textContent = dict[key];
    });
    document.querySelectorAll('[data-i18n-ph]').forEach(function(el){
      var key = el.getAttribute('data-i18n-ph');
      if (dict[key]) el.placeholder = dict[key];
    });
  });
});
function messageOnInstagram() {
  var msg = "My Access ID is " + ACCESS_ID + ", please approve it.";
  if (navigator.clipboard) { navigator.clipboard.writeText(msg).catch(function(){}); }
  window.open("https://instagram.com/" + IG_USER, "_blank");
}
var notifyBtn = document.getElementById('notifyBtn');
var nameInput = document.getElementById('visitorNameInput');
var statusEl = document.getElementById('notifyStatus');
notifyBtn.addEventListener('click', function() {
  var dict = I18N[currentLang];
  var name = nameInput.value.trim();
  if (!name) { statusEl.textContent = dict.needName; statusEl.className = 'notify-status err'; return; }
  notifyBtn.disabled = true;
  fetch(NOTIFY_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'name=' + encodeURIComponent(name)
  }).then(function(r){ return r.json(); }).then(function(data){
    if (data.ok) {
      statusEl.textContent = dict.sent; statusEl.className = 'notify-status';
    } else if (data.reason === 'cooldown') {
      statusEl.textContent = dict.wait; statusEl.className = 'notify-status err';
    } else {
      statusEl.textContent = dict.err; statusEl.className = 'notify-status err';
      notifyBtn.disabled = false;
    }
  }).catch(function(){
    statusEl.textContent = dict.err; statusEl.className = 'notify-status err';
    notifyBtn.disabled = false;
  });
});
setInterval(function() {
  fetch('/approval-status/' + encodeURIComponent(SLUG))
    .then(function(r){ return r.json(); })
    .then(function(data){
      if (data.approved || data.blocked) { window.location.reload(); }
    })
    .catch(function(){});
}, 6000);
%s
</script>
</body></html>`,
		astratoonixGateCSS(),
		astratoonixBootHTML(subjectTilesHTML),
		accessID, html.EscapeString(prefillName), url.QueryEscape(tgUser),
		accessID, igUser, slug,
		astratoonixBootJS(true),
	)
}

func (a *App) notifyNewAccessID(isNew bool, accessID int, fileName, visitorName string) {
	if !isNew || accessID == 0 || strings.TrimSpace(visitorName) == "" {
		return
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Approve", fmt.Sprintf("va:appr:%d", accessID)),
			tgbotapi.NewInlineKeyboardButtonData("🚫 Block", fmt.Sprintf("va:blk:%d", accessID)),
		),
	)
	a.pool.sendKB(a.cfg.AdminID, fmt.Sprintf(
		"🆕 New visitor — *%s*\n\nName: %s\nAccess ID: `%05d`",
		mdEscape(fileName), mdEscape(visitorName), accessID,
	), kb)
}

func (a *App) notifyVisitorRequest(accessID int, fileName, visitorName string) {
	if accessID == 0 || strings.TrimSpace(visitorName) == "" {
		return
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Approve", fmt.Sprintf("va:appr:%d", accessID)),
			tgbotapi.NewInlineKeyboardButtonData("🚫 Block", fmt.Sprintf("va:blk:%d", accessID)),
		),
	)
	a.pool.sendKB(a.cfg.AdminID, fmt.Sprintf(
		"📨 Visitor pinged you — *%s*\n\nName: %s\nAccess ID: `%05d`",
		mdEscape(fileName), mdEscape(visitorName), accessID,
	), kb)
}

func (a *App) handleApprovalStatus(w http.ResponseWriter, r *http.Request, slug string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")

	deviceID := getOrSetDeviceID(w, r)
	approval, _, err := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if err != nil || approval == nil {
		fmt.Fprint(w, `{"approved":false,"blocked":false}`)
		return
	}
	fmt.Fprintf(w, `{"approved":%t,"blocked":%t}`, approval.Approved, approval.Blocked)
}

func (a *App) handleNotify(w http.ResponseWriter, r *http.Request, slug string) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, `{"ok":false,"reason":"method"}`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	deviceID := getOrSetDeviceID(w, r)
	visitorName := strings.TrimSpace(r.FormValue("name"))
	if visitorName == "" {
		fmt.Fprint(w, `{"ok":false,"reason":"no_name"}`)
		return
	}

	approval, _, err := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if err != nil || approval == nil {
		fmt.Fprint(w, `{"ok":false,"reason":"error"}`)
		return
	}
	if approval.Blocked || approval.Approved {
		fmt.Fprint(w, `{"ok":true}`)
		return
	}
	if visitorName != approval.VisitorName {
		a.db.setApprovalName(ctx, deviceID, visitorName)
	}

	allowed, err := a.db.touchNotifyCooldown(ctx, deviceID, 2*time.Minute)
	if err != nil {
		fmt.Fprint(w, `{"ok":false,"reason":"error"}`)
		return
	}
	if !allowed {
		fmt.Fprint(w, `{"ok":false,"reason":"cooldown"}`)
		return
	}

	fileName := slug
	if rec, err := a.db.getFileByID(ctx, slug); err == nil {
		fileName = rec.FileName
	}
	a.notifyVisitorRequest(approval.AccessID, fileName, visitorName)
	fmt.Fprint(w, `{"ok":true}`)
}

func (a *App) handleProfile(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	deviceID := getOrSetDeviceID(w, r)

	if r.Method == http.MethodPost {
		clip := func(s string, n int) string {
			s = strings.TrimSpace(s)
			if len(s) > n {
				return s[:n]
			}
			return s
		}
		p := &VisitorProfile{
			DeviceID:  deviceID,
			Name:      clip(r.FormValue("name"), 100),
			About:     clip(r.FormValue("about"), 500),
			Email:     clip(r.FormValue("email"), 150),
			Phone:     clip(r.FormValue("phone"), 30),
			Instagram: clip(r.FormValue("instagram"), 60),
			Facebook:  clip(r.FormValue("facebook"), 60),
		}
		if err := a.db.upsertVisitorProfile(ctx, p); err != nil {
			a.logger.Warn("upsertVisitorProfile failed", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"ok":false}`)
			return
		}
		if p.Name != "" {
			a.db.setApprovalName(ctx, deviceID, p.Name)
		}
		fmt.Fprint(w, `{"ok":true}`)
		return
	}

	p, err := a.db.getVisitorProfile(ctx, deviceID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"ok":false}`)
		return
	}
	b, _ := json.Marshal(map[string]any{
		"ok": true, "name": p.Name, "about": p.About, "email": p.Email,
		"phone": p.Phone, "instagram": p.Instagram, "facebook": p.Facebook,
	})
	w.Write(b)
}

func (a *App) handleProfileLookup(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")

	if r.URL.Query().Get("token") != a.cfg.DashboardToken {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"ok":false,"reason":"forbidden"}`)
		return
	}
	accessID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("id")))
	if err != nil {
		fmt.Fprint(w, `{"ok":false,"reason":"bad_id"}`)
		return
	}
	rec, profile, err := a.db.getVisitorProfileByAccessID(ctx, accessID)
	if err != nil {
		fmt.Fprint(w, `{"ok":false,"reason":"not_found"}`)
		return
	}
	status := "pending"
	if rec.Blocked {
		status = "blocked"
	} else if rec.Approved {
		status = "approved"
	}
	resp := map[string]any{
		"ok": true, "accessId": rec.AccessID, "status": status, "approvalName": rec.VisitorName,
	}
	if profile != nil {
		resp["name"] = profile.Name
		resp["about"] = profile.About
		resp["email"] = profile.Email
		resp["phone"] = profile.Phone
		resp["instagram"] = profile.Instagram
		resp["facebook"] = profile.Facebook
	}
	b, _ := json.Marshal(resp)
	w.Write(b)
}

func (a *App) handleWatch(w http.ResponseWriter, r *http.Request, slug string) {
	ctx := r.Context()

	if slug == "" {
		http.Error(w, "Missing file ID", http.StatusBadRequest)
		return
	}

	var fileName, mimeType string
	var fileSize int64
	var expiresAt *time.Time
	var passwordHash *string
	var groupID *string

	cached := a.cache.getFile(ctx, slug)
	if cached != nil {
		fileName = cached.FileName
		fileSize = cached.FileSize
		mimeType = cached.MimeType
		expiresAt = cached.ExpiresAt
		passwordHash = cached.PasswordHash
		groupID = cached.GroupID
	} else {
		rec, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			http.Error(w, "File not found.", http.StatusNotFound)
			return
		}
		fileName = rec.FileName
		fileSize = rec.FileSize
		mimeType = rec.MimeType
		expiresAt = rec.ExpiresAt
		passwordHash = rec.PasswordHash
		groupID = rec.GroupID
		a.cache.setFile(ctx, slug, &cachedFile{
			MessageID: rec.MessageID, ChannelID: rec.ChannelID,
			FileName: rec.FileName, FileSize: rec.FileSize, MimeType: rec.MimeType,
			ExpiresAt: expiresAt, PasswordHash: passwordHash,
			GroupID: rec.GroupID, QualityLabel: rec.QualityLabel, QualityRank: rec.QualityRank,
		})
	}

	if isExpired(expiresAt) {
		http.Error(w, "⏳ This link has expired.", http.StatusGone)
		return
	}

	if !hasSplashCookie(r) {
		setSplashCookie(w)
		renderSplash(w, a.cfg.SplashAboutText, r.URL.String())
		return
	}

	deviceID := getOrSetDeviceID(w, r)

	if blockedApproval, _, _ := a.db.getOrCreateApproval(ctx, deviceID, slug); blockedApproval != nil && blockedApproval.Blocked {
		http.Error(w, "🚫 Access blocked.", http.StatusForbidden)
		return
	}

	if passwordHash != nil && *passwordHash != "" {
		visitorName := strings.TrimSpace(r.FormValue("name"))
		unlocked := hasValidPasswordCookie(r, slug, *passwordHash)
		if !unlocked {
			if pw := r.FormValue("pw"); pw != "" {
				if sha256Hex(pw) == *passwordHash {
					setPasswordCookie(w, slug, *passwordHash)
					unlocked = true
					a.db.getOrCreateApproval(ctx, deviceID, slug)
					if visitorName != "" {
						if err := a.db.setApprovalName(ctx, deviceID, visitorName); err != nil {
							a.logger.Warn("setApprovalName failed", zap.Error(err))
						}
					}
				} else {
					approval, isNewDevice, _ := a.db.getOrCreateApproval(ctx, deviceID, slug)
					accessID := 0
					if approval != nil {
						accessID = approval.AccessID
					}
					a.notifyNewAccessID(isNewDevice, accessID, fileName, visitorName)
					renderPasswordPrompt(w, slug, true, accessID, visitorName, a.cfg.PasswordPromptVideoURL, a.cfg.PasswordPromptImages, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername)
					return
				}
			}
		}
		if !unlocked {
			approval, isNewDevice, _ := a.db.getOrCreateApproval(ctx, deviceID, slug)
			accessID := 0
			if approval != nil {
				accessID = approval.AccessID
			}
			a.notifyNewAccessID(isNewDevice, accessID, fileName, visitorName)
			renderPasswordPrompt(w, slug, false, accessID, visitorName, a.cfg.PasswordPromptVideoURL, a.cfg.PasswordPromptImages, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername)
			return
		}
	}

	approval, isNewDevice, err := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if err != nil {
		a.logger.Warn("approval lookup failed", zap.Error(err))
	}
	if approval == nil || !approval.Approved {
		accessID := 0
		visitorName := ""
		if approval != nil {
			accessID = approval.AccessID
			visitorName = approval.VisitorName
		}
		a.notifyNewAccessID(isNewDevice, accessID, fileName, visitorName)
		renderPendingApproval(w, slug, accessID, visitorName, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername)
		return
	}

	if mimeType == "" {
		mimeType = "video/mp4"
	}

	isNew, views, viewErr := a.db.recordUniqueView(ctx, slug, deviceID)
	if viewErr != nil {
		a.logger.Warn("recordUniqueView failed", zap.Error(viewErr))
	} else if isNew {
		for _, m := range viewMilestones {
			if views == m {
				a.pool.sendMD(a.cfg.AdminID, fmt.Sprintf(
					"👀 *%s*\n\nhit *%s* unique views on the watch page\\!",
					mdEscape(fileName), mdEscape(formatCount(views)),
				))
				break
			}
		}
	}

	base := a.cfg.baseURL()
	streamURL := fmt.Sprintf("%s/stream/%s", base, slug)
	dlURL := fmt.Sprintf("%s/dl/%s", base, slug)
	watchURL := fmt.Sprintf("%s/watch/%s", base, slug)

	data := WatchData{
		FileName:    fileName,
		FileSize:    formatSize(fileSize),
		MimeType:    mimeType,
		StreamURL:   streamURL,
		DownloadURL: dlURL,
		WatchURL:    watchURL,
		ViewCount:   formatCount(views),
		LiveCount:   fmt.Sprintf("%d", a.cache.liveCount(ctx, slug)),
		DeviceID:    deviceID,
		Slug:        slug,
	}
	if ad := a.cache.getAdvertise(ctx); ad != nil {
		data.AdEnabled = true
		data.AdType = ad.Type
		data.AdURL = ad.URL
	}

	data.QualitiesJSON = template.JS("[]")
	if groupID != nil && *groupID != "" {
		if members, gErr := a.db.listGroupFiles(ctx, *groupID); gErr == nil && len(members) > 1 {
			opts := make([]qualityOption, 0, len(members))
			for _, m := range members {
				label := m.QualityLabel
				if label == "" {
					label = "SD"
				}
				opts = append(opts, qualityOption{
					Label:  label,
					URL:    fmt.Sprintf("%s/stream/%s", base, m.ID),
					Active: m.ID == slug,
				})
			}
			if b, mErr := json.Marshal(opts); mErr == nil {
				data.QualitiesJSON = template.JS(b)
			}
		}
	}

	tmpl, err := template.ParseFiles("/index.html")
	if err != nil {
		tmpl, err = template.ParseFiles("index.html")
	}
	if err != nil {
		a.logger.Error("watch template parse failed", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		a.logger.Error("watch template execute failed", zap.Error(err))
	}
}

func (a *App) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	w.Header().Set("Content-Type", "application/json")
	if len(q) < 2 {
		fmt.Fprint(w, `{"results":[]}`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	files, err := a.db.searchFiles(ctx, q, 40)
	if err != nil {
		a.logger.Error("search failed", zap.Error(err))
		http.Error(w, `{"error":"search failed"}`, http.StatusInternalServerError)
		return
	}
	type result struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Size      string `json:"size"`
		Locked    bool   `json:"locked"`
		Qualities int    `json:"qualities"`
	}
	deduped, counts := dedupeByGroup(files)
	if len(deduped) > 10 {
		deduped = deduped[:10]
	}
	out := make([]result, 0, len(deduped))
	for _, f := range deduped {
		out = append(out, result{
			ID:        f.ID,
			Name:      f.FileName,
			Size:      formatSize(f.FileSize),
			Locked:    f.PasswordHash != nil && *f.PasswordHash != "",
			Qualities: counts[f.ID],
		})
	}
	b, _ := json.Marshal(map[string]any{"results": out})
	w.Write(b)
}

// dedupeByGroup collapses multi-quality upload groups (same GroupID) down to
// a single representative FileRecord — the highest quality variant — while
// preserving the original ordering (by views/recency) and counting how many
// quality variants exist in each group. Without this, every quality variant
// of the same upload (/u ... /d session) showed up as its own separate card
// on the homepage rows, even though the player already lets a viewer switch
// quality from a single card via GroupID.
func dedupeByGroup(files []*FileRecord) ([]*FileRecord, map[string]int) {
	repByKey := make(map[string]*FileRecord)
	countByKey := make(map[string]int)
	order := make([]string, 0, len(files))

	keyFor := func(f *FileRecord) string {
		if f.GroupID != nil && *f.GroupID != "" {
			return "g:" + *f.GroupID
		}
		return "f:" + f.ID
	}

	for _, f := range files {
		key := keyFor(f)
		countByKey[key]++
		existing, ok := repByKey[key]
		if !ok {
			repByKey[key] = f
			order = append(order, key)
			continue
		}
		if f.QualityRank > existing.QualityRank {
			repByKey[key] = f
		}
	}

	out := make([]*FileRecord, 0, len(order))
	counts := make(map[string]int, len(order))
	for _, key := range order {
		rep := repByKey[key]
		out = append(out, rep)
		counts[rep.ID] = countByKey[key]
	}
	return out, counts
}

func (a *App) handleRows(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	const rowLimit = 12
	// Fetch a larger raw batch than we need, since multi-quality groups will
	// collapse down to one card each after dedupeByGroup.
	const rawFetchLimit = 60

	type rowItem struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Size      string `json:"size"`
		Locked    bool   `json:"locked"`
		Qualities int    `json:"qualities"`
	}
	toRows := func(files []*FileRecord) []rowItem {
		deduped, counts := dedupeByGroup(files)
		if len(deduped) > rowLimit {
			deduped = deduped[:rowLimit]
		}
		out := make([]rowItem, 0, len(deduped))
		for _, f := range deduped {
			out = append(out, rowItem{
				ID:        f.ID,
				Name:      f.FileName,
				Size:      formatSize(f.FileSize),
				Locked:    f.PasswordHash != nil && *f.PasswordHash != "",
				Qualities: counts[f.ID],
			})
		}
		return out
	}

	trending, err := a.db.topFilesByViews(ctx, rawFetchLimit)
	if err != nil {
		a.logger.Error("topFilesByViews failed", zap.Error(err))
	}
	newest, err := a.db.newestFiles(ctx, rawFetchLimit)
	if err != nil {
		a.logger.Error("newestFiles failed", zap.Error(err))
	}

	b, _ := json.Marshal(map[string]any{
		"trending":     toRows(trending),
		"new_releases": toRows(newest),
	})
	w.Write(b)
}

func (a *App) handleSubjects(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")

	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		subjects, err := a.db.listSubjects(ctx)
		if err != nil {
			a.logger.Error("listSubjects failed", zap.Error(err))
			http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
			return
		}
		b, _ := json.Marshal(map[string]any{"subjects": subjects})
		w.Write(b)
		return
	}

	files, err := a.db.listFilesBySubject(ctx, name)
	if err != nil {
		a.logger.Error("listFilesBySubject failed", zap.Error(err))
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
		return
	}
	type item struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Size      string `json:"size"`
		Locked    bool   `json:"locked"`
		Chapter   string `json:"chapter"`
		Qualities int    `json:"qualities"`
	}
	deduped, counts := dedupeByGroup(files)
	out := make([]item, 0, len(deduped))
	for _, f := range deduped {
		out = append(out, item{
			ID:        f.ID,
			Name:      f.FileName,
			Size:      formatSize(f.FileSize),
			Locked:    f.PasswordHash != nil && *f.PasswordHash != "",
			Chapter:   f.Chapter,
			Qualities: counts[f.ID],
		})
	}
	b, _ := json.Marshal(map[string]any{"subject": name, "files": out})
	w.Write(b)
}

func (a *App) handleYears(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")

	yearParam := strings.TrimSpace(r.URL.Query().Get("year"))
	if yearParam == "" {
		years, err := a.db.listYears(ctx)
		if err != nil {
			a.logger.Error("listYears failed", zap.Error(err))
			http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
			return
		}
		b, _ := json.Marshal(map[string]any{"years": years})
		w.Write(b)
		return
	}

	year, convErr := strconv.Atoi(yearParam)
	if convErr != nil {
		http.Error(w, `{"error":"bad year"}`, http.StatusBadRequest)
		return
	}
	files, err := a.db.listFilesByYear(ctx, year)
	if err != nil {
		a.logger.Error("listFilesByYear failed", zap.Error(err))
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
		return
	}
	type item struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Size      string `json:"size"`
		Locked    bool   `json:"locked"`
		Episode   string `json:"episode"`
		Qualities int    `json:"qualities"`
	}
	deduped, counts := dedupeByGroup(files)
	out := make([]item, 0, len(deduped))
	for _, f := range deduped {
		out = append(out, item{
			ID:        f.ID,
			Name:      f.FileName,
			Size:      formatSize(f.FileSize),
			Locked:    f.PasswordHash != nil && *f.PasswordHash != "",
			Episode:   f.EpisodeLabel,
			Qualities: counts[f.ID],
		})
	}
	b, _ := json.Marshal(map[string]any{"year": year, "files": out})
	w.Write(b)
}

func (a *App) handleAdmin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token := r.URL.Query().Get("token")
	if token != a.cfg.DashboardToken {
		http.Error(w, "❌ Invalid or missing token.", http.StatusForbidden)
		return
	}

	redirectBack := func() {
		http.Redirect(w, r, "/admin?token="+url.QueryEscape(token), http.StatusSeeOther)
	}
	if idStr := r.URL.Query().Get("approve"); idStr != "" {
		if id, err := strconv.Atoi(idStr); err == nil {
			a.db.approveByID(ctx, id)
		}
		redirectBack()
		return
	}
	if idStr := r.URL.Query().Get("block"); idStr != "" {
		if id, err := strconv.Atoi(idStr); err == nil {
			a.db.blockByID(ctx, id, true)
		}
		redirectBack()
		return
	}
	if idStr := r.URL.Query().Get("unblock"); idStr != "" {
		if id, err := strconv.Atoi(idStr); err == nil {
			a.db.blockByID(ctx, id, false)
		}
		redirectBack()
		return
	}
	if fid := r.URL.Query().Get("delfile"); fid != "" {
		if found, err := a.db.deleteFileByID(ctx, fid); err == nil && found {
			a.cache.delFile(ctx, fid)
		}
		redirectBack()
		return
	}

	files, _ := a.db.countFiles(ctx)
	users, _ := a.db.countUsers(ctx)
	totalViews, _ := a.db.sumViews(ctx)
	liveNow := a.cache.liveCountAll(ctx)
	top, _ := a.db.topFilesByViews(ctx, 25)
	visitors, _ := a.db.listApprovals(ctx, 30)
	tok := url.QueryEscape(token)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RAJ Admin Dashboard</title>
<style>
body{background:#0f1117;color:#e6e6e6;font-family:system-ui,sans-serif;margin:0;padding:24px;}
h1{font-size:20px;margin-bottom:4px;} .sub{color:#8b949e;font-size:12px;margin-bottom:24px;}
h2{font-size:15px;margin:30px 0 10px;color:#c9d1d9;}
.cards{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:28px;}
.card{background:#1b1c26;border-radius:12px;padding:16px 20px;min-width:120px;}
.card .n{font-size:26px;font-weight:700;color:#7aa2f7;} .card .l{font-size:12px;color:#8b949e;}
.live{color:#f87171;}
table{width:100%%;border-collapse:collapse;font-size:13px;}
th,td{padding:8px 10px;border-bottom:1px solid #262837;text-align:left;}
th{color:#8b949e;font-weight:600;} .locked{color:#f0b429;} .pw{color:#7ee787;font-family:monospace;}
.tag{color:#7aa2f7;font-size:12px;} .notag{color:#4b5263;}
.pill{display:inline-block;padding:4px 10px;border-radius:20px;font-size:12px;font-weight:600;
text-decoration:none;margin-right:4px;white-space:nowrap;}
.pill-appr{background:#1a3a2a;color:#7ee787;} .pill-blk{background:#3a1a1a;color:#ff7b86;}
.pill-unblk{background:#1a2a3a;color:#7aa2f7;} .pill-del{background:#3a1a1a;color:#ff7b86;}
.status-pending{color:#f0b429;} .status-approved{color:#7ee787;} .status-blocked{color:#ff7b86;}
.empty{color:#4b5263;font-size:13px;padding:10px 0;}
</style></head><body>
<h1>🖥️ RAJ Admin Dashboard</h1>
<div class="sub">Auto-refreshes every 10s</div>
<div class="cards">
<div class="card"><div class="n">%d</div><div class="l">📁 Files</div></div>
<div class="card"><div class="n">%d</div><div class="l">👥 Users</div></div>
<div class="card"><div class="n">%d</div><div class="l">🤖 Bots</div></div>
<div class="card"><div class="n">%s</div><div class="l">👁 Total unique views</div></div>
<div class="card"><div class="n live">%d</div><div class="l">🔴 Watching right now</div></div>
</div>

<h2>👥 Recent Visitors</h2>
<table><thead><tr><th>Access ID</th><th>Name</th><th>Status</th><th>Actions</th></tr></thead><tbody>`,
		files, users, a.pool.count(), formatCount(totalViews), liveNow)

	if len(visitors) == 0 {
		fmt.Fprint(w, `<tr><td colspan="4" class="empty">Koi visitor abhi tak nahi aaya.</td></tr>`)
	}
	for _, v := range visitors {
		name := v.VisitorName
		if name == "" {
			name = "—"
		}
		status, actions := `<span class="status-pending">⏳ Pending</span>`,
			fmt.Sprintf(`<a class="pill pill-appr" href="/admin?token=%s&amp;approve=%d">✅ Approve</a><a class="pill pill-blk" href="/admin?token=%s&amp;block=%d">🚫 Block</a>`,
				tok, v.AccessID, tok, v.AccessID)
		if v.Blocked {
			status = `<span class="status-blocked">🚫 Blocked</span>`
			actions = fmt.Sprintf(`<a class="pill pill-unblk" href="/admin?token=%s&amp;unblock=%d">✅ Unblock</a>`, tok, v.AccessID)
		} else if v.Approved {
			status = `<span class="status-approved">✅ Approved</span>`
			actions = fmt.Sprintf(`<a class="pill pill-blk" href="/admin?token=%s&amp;block=%d">🚫 Block</a>`, tok, v.AccessID)
		}
		fmt.Fprintf(w, `<tr><td>%05d</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
			v.AccessID, template.HTMLEscapeString(name), status, actions)
	}
	fmt.Fprint(w, `</tbody></table>

<h2>📁 Files</h2>
<table><thead><tr><th>File</th><th>Subject</th><th>Size</th><th>Views</th><th>Live now</th><th>🔒 Password</th><th>Uploaded</th><th>Actions</th></tr></thead><tbody>`)

	for _, f := range top {
		lock := `<span style="color:#4b5263;">—</span>`
		if f.PasswordHash != nil && *f.PasswordHash != "" {
			pw := "?"
			if f.PasswordPlain != nil && *f.PasswordPlain != "" {
				pw = *f.PasswordPlain
			}
			lock = fmt.Sprintf(`<span class="locked">🔒</span> <span class="pw">%s</span>`, template.HTMLEscapeString(pw))
		}
		tagLabel := `<span class="notag">untagged</span>`
		if f.Subject != "" {
			tagLabel = fmt.Sprintf(`<span class="tag">%s</span>`, template.HTMLEscapeString(f.Subject))
			if f.Chapter != "" {
				tagLabel += fmt.Sprintf(` <span class="notag">/ %s</span>`, template.HTMLEscapeString(f.Chapter))
			}
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%s</td>
<td><a class="pill pill-del" href="/admin?token=%s&amp;delfile=%s" onclick="return confirm('Delete %s permanently? Link ban jaayega dead.');">🗑️ Delete</a></td></tr>`,
			template.HTMLEscapeString(f.FileName), tagLabel, formatSize(f.FileSize), formatCount(f.ViewCount),
			a.cache.liveCount(ctx, f.ID), lock, f.CreatedAt.Format("02 Jan 15:04"),
			tok, url.QueryEscape(f.ID), template.JSEscapeString(f.FileName))
	}

	fmt.Fprint(w, `</tbody></table></body></html>`)
}

// ============================================================
// HELPERS
// ============================================================

var qualityPatternRe = regexp.MustCompile(`(?i)(240|360|480|720|1080|1440|2160)p|\b4k\b`)

// detectQuality tries to read a resolution/quality label out of a filename,
// e.g. "Movie.720p.mkv" -> ("720p", 720). Returns ("", 0) if nothing matches.
func detectQuality(filename string) (label string, rank int) {
	m := qualityPatternRe.FindStringSubmatch(filename)
	if m == nil {
		return "", 0
	}
	if m[1] == "" {
		return "4K", 2160
	}
	rank, _ = strconv.Atoi(m[1])
	return fmt.Sprintf("%dp", rank), rank
}

func makeShortHash(fileName string, fileSize int64, messageID int) string {
	key := fmt.Sprintf("%s-%d-%d", fileName, fileSize, messageID)
	h := 0
	for _, c := range key {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return fmt.Sprintf("%06x", h%0xffffff)[:6]
}

func toInternalChannelID(id int64) int64 {
	if id < -1000000000000 {
		return -(id + 1000000000000)
	}
	if id < 0 {
		return -id
	}
	return id
}

func mdEscape(s string) string {
	r := strings.NewReplacer(
		"_", "\\_", "*", "\\*", "[", "\\[", "]", "\\]",
		"(", "\\(", ")", "\\)", "~", "\\~", "`", "\\`",
		">", "\\>", "#", "\\#", "+", "\\+", "-", "\\-",
		"=", "\\=", "|", "\\|", "{", "\\{", "}", "\\}",
		".", "\\.", "!", "\\!",
	)
	return r.Replace(s)
}

var viewMilestones = []int64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000}

func formatSize(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func isExpired(expiresAt *time.Time) bool {
	return expiresAt != nil && time.Now().After(*expiresAt)
}

func formatCount(n int64) string {
	switch {
	case n < 1000:
		return fmt.Sprintf("%d", n)
	case n < 1000000:
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	default:
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
}

func parseExpiryDuration(s string) (dur time.Duration, clear bool, err error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "off" || s == "never" || s == "remove" || s == "none" {
		return 0, true, nil
	}
	if s == "" {
		return 0, false, fmt.Errorf("empty duration")
	}
	if strings.HasSuffix(s, "d") {
		n, e := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if e != nil {
			return 0, false, fmt.Errorf("invalid days value")
		}
		return time.Duration(n * 24 * float64(time.Hour)), false, nil
	}
	if strings.HasSuffix(s, "y") {
		n, e := strconv.ParseFloat(strings.TrimSuffix(s, "y"), 64)
		if e != nil {
			return 0, false, fmt.Errorf("invalid years value")
		}
		return time.Duration(n * 365 * 24 * float64(time.Hour)), false, nil
	}
	d, e := time.ParseDuration(s)
	if e != nil {
		return 0, false, fmt.Errorf("invalid duration (use e.g. 30m, 12h, 7d, 1y, or 'off')")
	}
	return d, false, nil
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return sha256Hex(fmt.Sprintf("fallback-%d", time.Now().UnixNano()))
	}
	return hex.EncodeToString(b)
}

const deviceCookieName = "rdid"
const splashCookieName = "rsplash"

func passwordCookieName(slug string) string { return "fpw_" + slug }

func getOrSetDeviceID(w http.ResponseWriter, r *http.Request) string {
	if ck, err := r.Cookie(deviceCookieName); err == nil && ck.Value != "" {
		return ck.Value
	}
	id := uuid.New().String()
	http.SetCookie(w, &http.Cookie{
		Name:     deviceCookieName,
		Value:    id,
		Path:     "/",
		MaxAge:   10 * 365 * 24 * 60 * 60,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	return id
}

func hasValidPasswordCookie(r *http.Request, slug, hash string) bool {
	ck, err := r.Cookie(passwordCookieName(slug))
	return err == nil && ck.Value == hash
}

func setPasswordCookie(w http.ResponseWriter, slug, hash string) {
	http.SetCookie(w, &http.Cookie{
		Name:     passwordCookieName(slug),
		Value:    hash,
		Path:     "/",
		MaxAge:   60,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
}

func hasSplashCookie(r *http.Request) bool {
	ck, err := r.Cookie(splashCookieName)
	return err == nil && ck.Value == "1"
}

func setSplashCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     splashCookieName,
		Value:    "1",
		Path:     "/",
		MaxAge:   10 * 365 * 24 * 60 * 60,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
}

func sanitizeName(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, `"`, "")
	if name == "" || name == "." {
		return "file"
	}
	return name
}

// ============================================================
// MAIN
// ============================================================

func main() {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Encoding:         "console",
		EncoderConfig:    encCfg,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}.Build()
	defer logger.Sync()

	fmt.Print("\033[36m\n██████╗  █████╗      ██╗\n██╔══██╗██╔══██╗     ██║\n██████╔╝███████║     ██║\n██╔══██╗██╔══██║██   ██║\n██║  ██║██║  ██║╚█████╔╝\n╚═╝  ╚═╝╚═╝  ╚═╝ ╚════╝\n\033[0m  📚 Astratoonix Education Platform\n  Built by Raj Dev\n\n")
	logger.Info("📚 Astratoonix Education Platform starting...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatal("config error", zap.Error(err))
	}
	logger.Info("config loaded",
		zap.Int("bots", len(cfg.BotTokens)),
		zap.String("fqdn", cfg.FQDN),
	)

	db, err := newDB(ctx, cfg.DBURI)
	if err != nil {
		logger.Fatal("database error", zap.Error(err))
	}
	defer db.close()
	logger.Info("✅ database connected (MongoDB)")

	cache, err := newCache(ctx, cfg.RedisURI)
	if err != nil {
		logger.Fatal("redis error", zap.Error(err))
	}
	defer cache.close()
	logger.Info("✅ redis connected")

	pool, err := newBotPool(cfg.BotTokens, logger)
	if err != nil {
		logger.Fatal("bot pool error", zap.Error(err))
	}

	pool.primary().Request(tgbotapi.NewSetMyCommands(
		tgbotapi.BotCommand{Command: "start", Description: "Start"},
		tgbotapi.BotCommand{Command: "help", Description: "Help"},
		tgbotapi.BotCommand{Command: "stats", Description: "Stats (admin)"},
		tgbotapi.BotCommand{Command: "dashboard", Description: "Admin dashboard link (admin)"},
		tgbotapi.BotCommand{Command: "setpass", Description: "Password-protect a link"},
		tgbotapi.BotCommand{Command: "tag", Description: "Tag a file with Subject/Chapter (admin)"},
	))

	logger.Info("starting MTProto connections...")
	mtPool := newMTProtoPool(cfg.APIID, cfg.APIHash, []string{cfg.MainBotToken}, cfg.StringSession, logger)

	for i := 0; i < 15; i++ {
		if mtPool.isAnyReady() {
			logger.Info("✅ MTProto ready — any file size supported!")
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !mtPool.isAnyReady() {
		logger.Warn("⚠️ MTProto still connecting — will retry in background")
	}

	app := &App{
		cfg:    cfg,
		db:     db,
		cache:  cache,
		pool:   pool,
		mtPool: mtPool,
		logger: logger,
	}

	dashboardLink := fmt.Sprintf("%s/admin?token=%s", cfg.baseURL(), cfg.DashboardToken)
	app.pool.send(cfg.AdminID, fmt.Sprintf(
		"🚀 Bot started!\n\n🖥️ Dashboard: %s", dashboardLink,
	))

	mux := http.NewServeMux()
	mux.HandleFunc("/search", app.handleSearch)
	mux.HandleFunc("/rows", app.handleRows)
	mux.HandleFunc("/subjects", app.handleSubjects)
	mux.HandleFunc("/years", app.handleYears)
	mux.HandleFunc("/profile", app.handleProfile)
	mux.HandleFunc("/profile/lookup", app.handleProfileLookup)
	mux.HandleFunc("/", app.serveHTTP)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		logger.Info("✅ HTTP server started", zap.String("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http error", zap.Error(err))
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		staleAfter := 30 * time.Minute
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
				n, err := app.db.deletePendingApprovals(cleanupCtx, &staleAfter)
				cleanupCancel()
				if err != nil {
					logger.Warn("pending approval cleanup failed", zap.Error(err))
				} else if n > 0 {
					logger.Info("cleaned up stale pending visitors", zap.Int64("count", n))
				}
			}
		}
	}()

	go func() {
		logger.Info("✅ bot polling started", zap.String("bot", "@"+pool.primary().Self.UserName))

		for {
			select {
			case <-ctx.Done():
				pool.stopUpdates()
				return
			default:
			}

			u := tgbotapi.NewUpdate(0)
			u.Timeout = 60
			pool.primary().Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: true})
			updates := pool.primary().GetUpdatesChan(u)

			pollLoop:
			for {
				select {
				case <-ctx.Done():
					pool.stopUpdates()
					return
				case update, ok := <-updates:
					if !ok {
						break pollLoop
					}
					go func(upd tgbotapi.Update) {
						defer func() {
							if r := recover(); r != nil {
								logger.Error("panic", zap.Any("r", r))
							}
						}()
						app.dispatch(ctx, upd)
					}(update)
				}
			}

			logger.Warn("polling stopped, retrying in 5s...")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
	logger.Info("bye! ✓")
}
