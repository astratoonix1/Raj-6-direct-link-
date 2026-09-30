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
	PremiumQRURL             string
	ReferralWelcomeSlug      string
	OwnerName                string
	OwnerImageURL            string
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
	// No hardcoded fallback stream ID here anymore — an old placeholder ID
	// that doesn't exist in the database was causing the password-prompt
	// video to render as a permanent black box. With no URL set, the video
	// block is simply skipped (see renderPasswordPrompt) until the admin
	// sets one live via /video on Telegram, or this env var.

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

	cfg.PremiumQRURL = strings.TrimSpace(os.Getenv("PREMIUM_QR_URL"))
	if cfg.PremiumQRURL == "" {
		cfg.PremiumQRURL = "https://i.ibb.co/ccNPj8YW/Screenshot-20260828-134816-GPay.png"
	}

	// The video a fresh referral link lands on after the splash/login step.
	// Configurable so if this specific file is ever deleted, the admin can
	// just point it at a different file_id via env var — no code change or
	// redeploy needed.
	cfg.ReferralWelcomeSlug = strings.TrimSpace(os.Getenv("REFERRAL_WELCOME_SLUG"))
	if cfg.ReferralWelcomeSlug == "" {
		cfg.ReferralWelcomeSlug = "2ddcc463-cf7f-4eb9-a9aa-f61ecca41500"
	}

	cfg.OwnerName = strings.TrimSpace(os.Getenv("OWNER_NAME"))
	if cfg.OwnerName == "" {
		cfg.OwnerName = "Raj Dev"
	}
	cfg.OwnerImageURL = strings.TrimSpace(os.Getenv("OWNER_IMAGE_URL"))

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
	Description   string     `bson:"description,omitempty" json:"description,omitempty"`
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

// --- Referral system: 10 referrals = 1 day of premium ---
// ReferralCount tracks one referrer's running total. ReferredDevice records
// each NEW visitor who arrived via a referral link, keyed by their own
// device_id — this is the dedupe guard so the same referred device can
// never be counted twice (e.g. clicking the link again, revisiting, etc.).
type ReferralCount struct {
	DeviceID string `bson:"_id" json:"device_id"`
	Count    int    `bson:"count" json:"count"`
	Rewarded int    `bson:"rewarded" json:"rewarded"` // how many 10-blocks already paid out
}

type ReferredDevice struct {
	DeviceID   string    `bson:"_id" json:"device_id"`
	ReferrerID string    `bson:"referrer_id" json:"referrer_id"`
	CreatedAt  time.Time `bson:"created_at" json:"created_at"`
}

// TeamMember is a purely cosmetic "showcase" entry — added via /admin on
// Telegram, it just displays a name + photo on the website with an "Admin"
// badge. It grants ZERO real bot permissions; only a Telegram user whose ID
// matches Config.AdminID can ever run admin-only commands, regardless of
// what's shown here. This exists so the owner can publicly credit/highlight
// people (moderators, helpers, friends) without touching real access.
type TeamMember struct {
	Name      string    `bson:"_id" json:"name"`
	ImageURL  string    `bson:"image_url" json:"image_url"`
	AddedAt   time.Time `bson:"added_at" json:"added_at"`
}

type DB struct {
	client          *mongo.Client
	db              *mongo.Database
	files           *mongo.Collection
	users           *mongo.Collection
	approvals       *mongo.Collection
	visitorProfiles *mongo.Collection
	referrals       *mongo.Collection
	referredDevices *mongo.Collection
	teamMembers     *mongo.Collection
	fileViews       *mongo.Collection
	premiumCodes    *mongo.Collection
	premiumRequests *mongo.Collection
	premiumDevices  *mongo.Collection
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
		referrals:       database.Collection("referrals"),
		referredDevices: database.Collection("referred_devices"),
		teamMembers:     database.Collection("team_members"),
		fileViews:       database.Collection("file_views"),
		premiumCodes:    database.Collection("premium_codes"),
		premiumRequests: database.Collection("premium_requests"),
		premiumDevices:  database.Collection("premium_devices"),
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

// --- Premium (bypasses per-file password protection for a device) ---

type PremiumCode struct {
	Code         string     `bson:"_id" json:"code"`
	DurationDays int        `bson:"duration_days" json:"duration_days"`
	CreatedAt    time.Time  `bson:"created_at" json:"created_at"`
	CreatedBy    int64      `bson:"created_by" json:"created_by"`
	Redeemed     bool       `bson:"redeemed" json:"redeemed"`
	RedeemedBy   string     `bson:"redeemed_by,omitempty" json:"redeemed_by,omitempty"`
	RedeemedAt   *time.Time `bson:"redeemed_at,omitempty" json:"redeemed_at,omitempty"`
	// Multi-use / flexible-duration support. Old codes don't have these
	// fields: missing MaxUses means 1 use, missing DurationMinutes means
	// DurationDays is used.
	DurationMinutes int      `bson:"duration_minutes,omitempty" json:"duration_minutes,omitempty"`
	MaxUses         int      `bson:"max_uses,omitempty" json:"max_uses,omitempty"`
	UsedCount       int      `bson:"used_count,omitempty" json:"used_count,omitempty"`
	RedeemedDevices []string `bson:"redeemed_devices,omitempty" json:"redeemed_devices,omitempty"`
}

type PremiumDevice struct {
	DeviceID  string    `bson:"_id" json:"device_id"`
	ExpiresAt time.Time `bson:"expires_at" json:"expires_at"`
	GrantedAt time.Time `bson:"granted_at" json:"granted_at"`
}

func (db *DB) createPremiumCode(ctx context.Context, code string, durationDays int, createdBy int64) error {
	return db.createPremiumCodeAdv(ctx, code, durationDays, 0, 1, createdBy)
}

// createPremiumCodeAdv creates a code with a flexible duration (durationMinutes
// > 0 overrides durationDays) and a cap on how many different devices can
// redeem it (maxUses, minimum 1).
func (db *DB) createPremiumCodeAdv(ctx context.Context, code string, durationDays, durationMinutes, maxUses int, createdBy int64) error {
	if maxUses < 1 {
		maxUses = 1
	}
	_, err := db.premiumCodes.InsertOne(ctx, &PremiumCode{
		Code: code, DurationDays: durationDays, DurationMinutes: durationMinutes,
		MaxUses: maxUses, CreatedAt: time.Now(), CreatedBy: createdBy,
	})
	return err
}

// deletePremiumCode removes a not-yet-redeemed (or already-redeemed) code
// outright — used by /deletecode to invalidate a code that was generated
// by mistake or should no longer be usable.
func (db *DB) deletePremiumCode(ctx context.Context, code string) (bool, error) {
	res, err := db.premiumCodes.DeleteOne(ctx, bson.M{"_id": code})
	if err != nil {
		return false, err
	}
	return res.DeletedCount > 0, nil
}

// redeemPremiumCode atomically records one more use of a code and
// grants/extends premium on the given device. A code can be redeemed by up
// to MaxUses different devices (default 1); the same device can't use it
// twice. Returns the granted expiry, or an error if the code doesn't exist,
// is fully used, or this device already used it.
func (db *DB) redeemPremiumCode(ctx context.Context, code, deviceID string) (time.Time, error) {
	var pc PremiumCode
	err := db.premiumCodes.FindOneAndUpdate(ctx,
		bson.M{
			"_id":              code,
			"redeemed":         false,
			"redeemed_devices": bson.M{"$ne": deviceID},
			"$expr": bson.M{"$lt": bson.A{
				bson.M{"$ifNull": bson.A{"$used_count", 0}},
				bson.M{"$ifNull": bson.A{"$max_uses", 1}},
			}},
		},
		bson.M{
			"$inc":      bson.M{"used_count": 1},
			"$addToSet": bson.M{"redeemed_devices": deviceID},
			"$set":      bson.M{"redeemed_by": deviceID, "redeemed_at": time.Now()},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&pc)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid or already-used code")
	}
	maxUses := pc.MaxUses
	if maxUses < 1 {
		maxUses = 1
	}
	if pc.UsedCount >= maxUses {
		// Last allowed use consumed — mark the code fully redeemed.
		db.premiumCodes.UpdateOne(ctx, bson.M{"_id": code}, bson.M{"$set": bson.M{"redeemed": true}})
	}

	base := time.Now()
	if existing, gErr := db.getPremiumDevice(ctx, deviceID); gErr == nil && existing.ExpiresAt.After(base) {
		base = existing.ExpiresAt // extend on top of remaining time, don't waste it
	}
	var expiresAt time.Time
	if pc.DurationMinutes > 0 {
		expiresAt = base.Add(time.Duration(pc.DurationMinutes) * time.Minute)
	} else {
		expiresAt = base.AddDate(0, 0, pc.DurationDays)
	}

	_, err = db.premiumDevices.UpdateOne(ctx,
		bson.M{"_id": deviceID},
		bson.M{"$set": bson.M{"expires_at": expiresAt, "granted_at": time.Now()}},
		options.Update().SetUpsert(true),
	)
	return expiresAt, err
}

func (db *DB) grantPremiumDirect(ctx context.Context, deviceID string, days int) (time.Time, error) {
	base := time.Now()
	if existing, gErr := db.getPremiumDevice(ctx, deviceID); gErr == nil && existing.ExpiresAt.After(base) {
		base = existing.ExpiresAt
	}
	expiresAt := base.AddDate(0, 0, days)
	_, err := db.premiumDevices.UpdateOne(ctx,
		bson.M{"_id": deviceID},
		bson.M{"$set": bson.M{"expires_at": expiresAt, "granted_at": time.Now()}},
		options.Update().SetUpsert(true),
	)
	return expiresAt, err
}

// --- Premium purchase requests (website "Buy Premium" -> Telegram approval) ---
// Flow: visitor picks a plan on the website -> a pending request is stored
// and the admin gets a Telegram message with an "Approve" button -> tapping
// it generates a redeem code, immediately grants premium to that exact
// device (so the site can auto-detect it via polling with zero further
// action from the visitor), and still keeps the code around so the admin
// can manually hand it to the visitor through some other channel (e.g. the
// contact-message feature) if the automatic detection is ever missed
// (browser closed, cookies cleared, etc.).
type PremiumRequest struct {
	ID        string    `bson:"_id" json:"id"`
	DeviceID  string    `bson:"device_id" json:"device_id"`
	PlanID    string    `bson:"plan_id" json:"plan_id"`
	PlanLabel string    `bson:"plan_label" json:"plan_label"`
	Days      int       `bson:"days" json:"days"`
	Price     string    `bson:"price" json:"price"`
	Status    string    `bson:"status" json:"status"` // pending | approved
	Code      string    `bson:"code,omitempty" json:"code,omitempty"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func (db *DB) createPremiumRequest(ctx context.Context, pr *PremiumRequest) error {
	pr.CreatedAt = time.Now()
	pr.Status = "pending"
	_, err := db.premiumRequests.InsertOne(ctx, pr)
	return err
}

func (db *DB) getPremiumRequest(ctx context.Context, id string) (*PremiumRequest, error) {
	var pr PremiumRequest
	err := db.premiumRequests.FindOne(ctx, bson.M{"_id": id}).Decode(&pr)
	if err != nil {
		return nil, err
	}
	return &pr, nil
}

// approvePremiumRequest atomically flips a still-pending request to
// approved and stamps it with the generated code — returns false if it was
// already approved (e.g. admin double-tapped the button), so the caller
// doesn't grant premium or send confirmations twice.
func (db *DB) approvePremiumRequest(ctx context.Context, id, code string) (*PremiumRequest, bool, error) {
	var pr PremiumRequest
	err := db.premiumRequests.FindOneAndUpdate(ctx,
		bson.M{"_id": id, "status": "pending"},
		bson.M{"$set": bson.M{"status": "approved", "code": code}},
	).Decode(&pr)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, false, nil
		}
		return nil, false, err
	}
	pr.Status = "approved"
	pr.Code = code
	return &pr, true, nil
}

func (db *DB) getPremiumDevice(ctx context.Context, deviceID string) (*PremiumDevice, error) {
	var pd PremiumDevice
	err := db.premiumDevices.FindOne(ctx, bson.M{"_id": deviceID}).Decode(&pd)
	if err != nil {
		return nil, err
	}
	return &pd, nil
}

func (db *DB) isPremium(ctx context.Context, deviceID string) bool {
	pd, err := db.getPremiumDevice(ctx, deviceID)
	if err != nil {
		return false
	}
	return pd.ExpiresAt.After(time.Now())
}

func (db *DB) revokePremium(ctx context.Context, deviceID string) (bool, error) {
	res, err := db.premiumDevices.DeleteOne(ctx, bson.M{"_id": deviceID})
	if err != nil {
		return false, err
	}
	return res.DeletedCount > 0, nil
}

// recordReferral attributes a brand-new visitor to whoever's referral link
// they arrived through — but only once per referred device (guarded by
// referredDevices), so revisits or link re-clicks never double-count.
// Returns the referrer's new total and whether this referral just crossed
// another full block of 10 (i.e. earned another day of premium).
func (db *DB) recordReferral(ctx context.Context, referrerDeviceID, referredDeviceID string) (newCount int, justEarned bool, err error) {
	if referrerDeviceID == "" || referrerDeviceID == referredDeviceID {
		return 0, false, nil
	}
	_, err = db.referredDevices.InsertOne(ctx, &ReferredDevice{
		DeviceID: referredDeviceID, ReferrerID: referrerDeviceID, CreatedAt: time.Now(),
	})
	if err != nil {
		// Duplicate key (already recorded) or any other insert failure —
		// either way, don't count it again.
		return 0, false, nil
	}

	var rc ReferralCount
	err = db.referrals.FindOneAndUpdate(ctx,
		bson.M{"_id": referrerDeviceID},
		bson.M{"$inc": bson.M{"count": 1}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	).Decode(&rc)
	if err != nil {
		return 0, false, err
	}

	earnedBlocks := rc.Count / 10
	if earnedBlocks > rc.Rewarded {
		if _, uErr := db.referrals.UpdateOne(ctx, bson.M{"_id": referrerDeviceID}, bson.M{"$set": bson.M{"rewarded": earnedBlocks}}); uErr == nil {
			return rc.Count, true, nil
		}
	}
	return rc.Count, false, nil
}

func (db *DB) getReferralCount(ctx context.Context, deviceID string) int {
	var rc ReferralCount
	if err := db.referrals.FindOne(ctx, bson.M{"_id": deviceID}).Decode(&rc); err != nil {
		return 0
	}
	return rc.Count
}

// --- Team showcase (purely cosmetic, see TeamMember doc comment) ---

func (db *DB) addTeamMember(ctx context.Context, name, imageURL string) error {
	_, err := db.teamMembers.UpdateOne(ctx,
		bson.M{"_id": name},
		bson.M{"$set": bson.M{"image_url": imageURL, "added_at": time.Now()}},
		options.Update().SetUpsert(true),
	)
	return err
}

func (db *DB) removeTeamMember(ctx context.Context, name string) (bool, error) {
	res, err := db.teamMembers.DeleteOne(ctx, bson.M{"_id": name})
	if err != nil {
		return false, err
	}
	return res.DeletedCount > 0, nil
}

func (db *DB) listTeamMembers(ctx context.Context) ([]*TeamMember, error) {
	opts := options.Find().SetSort(bson.D{{Key: "added_at", Value: 1}})
	cursor, err := db.teamMembers.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*TeamMember
	return out, cursor.All(ctx, &out)
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
			"description":    f.Description,
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

func (db *DB) topFilesByViews(ctx context.Context, skip, limit int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "view_count", Value: -1}, {Key: "_id", Value: 1}}).SetSkip(int64(skip)).SetLimit(int64(limit))
	cursor, err := db.files.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) newestFiles(ctx context.Context, skip, limit int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: 1}}).SetSkip(int64(skip)).SetLimit(int64(limit))
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
	// Match either the manual /setyear tag, or the year written anywhere in
	// the file's text (name, description, subject, chapter, episode label).
	// The year must stand alone (not part of a longer number), so 2026
	// matches "Movie 2026", "Movie.2026.1080p" but not "12026" or "20261".
	yearRe := fmt.Sprintf(`(^|[^0-9])%d([^0-9]|$)`, year)
	filter := bson.M{"$or": []bson.M{
		{"year": year},
		{"file_name": bson.M{"$regex": yearRe}},
		{"description": bson.M{"$regex": yearRe}},
		{"subject": bson.M{"$regex": yearRe}},
		{"chapter": bson.M{"$regex": yearRe}},
		{"episode_label": bson.M{"$regex": yearRe}},
	}}
	cursor, err := db.files.Find(ctx, filter, opts)
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

// getApprovalByAccessID looks up the device tied to a short Access ID —
// used by /reply so the admin can address someone by the ID shown in a
// notification, without needing to know their raw device_id.
func (db *DB) getApprovalByAccessID(ctx context.Context, accessID int) (*ApprovalRecord, error) {
	var rec ApprovalRecord
	err := db.approvals.FindOne(ctx, bson.M{"access_id": accessID}).Decode(&rec)
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

// resolveDeviceRef accepts either a short numeric Access ID (as shown in
// /user, dashboard and notifications) or a raw device UUID, and returns the
// real device_id. ok=false means an Access ID was given but doesn't exist.
func (a *App) resolveDeviceRef(ctx context.Context, ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if n, err := strconv.Atoi(ref); err == nil && len(ref) <= 6 {
		ap, aErr := a.db.getApprovalByAccessID(ctx, n)
		if aErr != nil || ap == nil {
			return "", false
		}
		return ap.DeviceID, true
	}
	return ref, true
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
	c.client.Set(ctx, fmt.Sprintf("multiup:%d", userID), groupID, 2*time.Hour)
}

// refreshMultiUploadSession extends the session's TTL. Called every time a
// file is added to a multi-quality upload group so that large files
// (e.g. 1080p, which take a while to forward/store) don't cause the session
// to silently expire mid-upload — which previously caused later files to
// fall through to the single-upload path instead of joining the group,
// leaving the finished link with fewer qualities than were actually sent.
func (c *Cache) refreshMultiUploadSession(ctx context.Context, userID int64) {
	c.client.Expire(ctx, fmt.Sprintf("multiup:%d", userID), 2*time.Hour)
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
	c.client.Expire(ctx, key, 2*time.Hour)
}

func (c *Cache) clearMultiUploadEntries(ctx context.Context, groupID string) {
	c.client.Del(ctx, fmt.Sprintf("multiup:files:%s", groupID))
}

// contactMessageCooldown throttles the website's "Message Me" contact form
// to one submission per device per minute, so it can't be used to spam the
// admin's Telegram.
func (c *Cache) contactMessageCooldown(ctx context.Context, deviceID string) bool {
	key := "contactcd:" + deviceID
	ok, _ := c.client.SetNX(ctx, key, "1", time.Minute).Result()
	return ok
}

// setAdminReply / popAdminReply implement a tiny one-shot mailbox so an
// admin's reply (sent via /reply <access_id> <message>) can reach the
// specific visitor's browser — the page polls popAdminReply for its own
// device, and once delivered the reply is removed so it's shown once.
// setSiteAnnouncement / getSiteAnnouncement implement a single site-wide
// banner (not per-device) — set via /reply on Telegram, shown to every
// visitor. Simpler and more reliable than a per-device mailbox, since it
// doesn't depend on matching a specific browser's cookie/session; anyone
// polling the site sees it regardless of which device they're on.
func (c *Cache) setSiteAnnouncement(ctx context.Context, message string) {
	now := time.Now().Unix()
	c.client.Set(ctx, "site:announcement:text", message, 7*24*time.Hour)
	c.client.Set(ctx, "site:announcement:ts", fmt.Sprintf("%d", now), 7*24*time.Hour)
}

func (c *Cache) getSiteAnnouncement(ctx context.Context) (message string, updatedAt int64) {
	message, _ = c.client.Get(ctx, "site:announcement:text").Result()
	tsStr, _ := c.client.Get(ctx, "site:announcement:ts").Result()
	fmt.Sscanf(tsStr, "%d", &updatedAt)
	return message, updatedAt
}

func (c *Cache) clearSiteAnnouncement(ctx context.Context) {
	c.client.Del(ctx, "site:announcement:text", "site:announcement:ts")
}

// setCustomSection / getCustomSection / clearCustomSection implement a
// single site-wide raw HTML/CSS/JS block (set via /code on Telegram),
// rendered in a container at the bottom of every page on the website. No
// expiry: it stays until an admin clears it with /removecode.
func (c *Cache) setCustomSection(ctx context.Context, html string) {
	now := time.Now().Unix()
	c.client.Set(ctx, "site:customsection:html", html, 0)
	c.client.Set(ctx, "site:customsection:ts", fmt.Sprintf("%d", now), 0)
}

func (c *Cache) getCustomSection(ctx context.Context) (html string, updatedAt int64) {
	html, _ = c.client.Get(ctx, "site:customsection:html").Result()
	tsStr, _ := c.client.Get(ctx, "site:customsection:ts").Result()
	updatedAt, _ = strconv.ParseInt(tsStr, 10, 64)
	return html, updatedAt
}

func (c *Cache) clearCustomSection(ctx context.Context) {
	c.client.Del(ctx, "site:customsection:html", "site:customsection:ts")
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

// Password-prompt video (set via /video on Telegram). Same pending-upload
// pattern as /advertise: arm a 5-minute window, then the next video the
// admin sends is uploaded and its stream URL is stored, with no expiry,
// overriding PasswordPromptVideoURL until /video off clears it.
func (c *Cache) setPromptVideoPending(ctx context.Context, userID int64) {
	c.client.Set(ctx, fmt.Sprintf("promptvideo:pending:%d", userID), "1", 5*time.Minute)
}
func (c *Cache) isPromptVideoPending(ctx context.Context, userID int64) bool {
	v, err := c.client.Get(ctx, fmt.Sprintf("promptvideo:pending:%d", userID)).Result()
	return err == nil && v == "1"
}
func (c *Cache) clearPromptVideoPending(ctx context.Context, userID int64) {
	c.client.Del(ctx, fmt.Sprintf("promptvideo:pending:%d", userID))
}
func (c *Cache) setPromptVideo(ctx context.Context, url string) {
	c.client.Set(ctx, "site:promptvideo:url", url, 0)
}
func (c *Cache) getPromptVideo(ctx context.Context) (string, bool) {
	url, err := c.client.Get(ctx, "site:promptvideo:url").Result()
	return url, err == nil && url != ""
}
func (c *Cache) clearPromptVideo(ctx context.Context) {
	c.client.Del(ctx, "site:promptvideo:url")
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
		g                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
