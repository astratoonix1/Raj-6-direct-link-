package main

import (
	"bytes"
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
	TMDBAPIKey               string

	// Ad slots, set from env so the owner can paste his ad network's code
	// (Adsterra, Monetag, ...) without touching the source:
	//   AD_BANNER_HTML  - banner/native ad code, shown under the player
	//   AD_BODY_HTML    - popunder / social-bar script, added at the end of the page
	//   AD_DIRECT_LINK  - direct-link ad URL (used when /adlink is not set)
	//   AD_LINK_TEXT    - button text for AD_DIRECT_LINK
	AdBannerHTML string
	AdBodyHTML   string
	AdDirectLink string
	AdLinkText   string

	// Home page "WhatsApp" button: WHATSAPP_NUMBER (digits with country code,
	// e.g. 919876543210) and optional WHATSAPP_TEXT (pre-filled message).
	WhatsAppNumber string
	WhatsAppText   string
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
	// TMDB (themoviedb.org) — v3 API key or v4 read-access token. Needed only
	// for the /tmdb command; without it everything else works as before.
	cfg.TMDBAPIKey = strings.TrimSpace(os.Getenv("TMDB_API_KEY"))
	cfg.AdBannerHTML = strings.TrimSpace(os.Getenv("AD_BANNER_HTML"))
	cfg.AdBodyHTML = strings.TrimSpace(os.Getenv("AD_BODY_HTML"))
	// Owner's Adsterra script: on by default so it works without any env
	// setting. Set AD_BODY_HTML to other ad code to replace it, or to "off"
	// to switch it off.
	switch strings.ToLower(cfg.AdBodyHTML) {
	case "":
		cfg.AdBodyHTML = `<script src="https://pl32222386.profitableratecpmnetwork.com/b1/0c/12/b10c12ecccbf7d50c9358e2a7318732b.js"></script>`
	case "off":
		cfg.AdBodyHTML = ""
	}
	cfg.AdDirectLink = strings.TrimSpace(os.Getenv("AD_DIRECT_LINK"))
	cfg.AdLinkText = strings.TrimSpace(os.Getenv("AD_LINK_TEXT"))
	cfg.WhatsAppNumber = strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, os.Getenv("WHATSAPP_NUMBER"))
	cfg.WhatsAppText = strings.TrimSpace(os.Getenv("WHATSAPP_TEXT"))

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
	// Hidden marks files that exist only to be played from a specific,
	// non-browsable spot on the site (a /video or /video2 upload) — they
	// must never show up in the homepage rows, search, subject or year
	// listings, even though they're normal, individually-playable files.
	Hidden        bool       `bson:"hidden,omitempty" json:"hidden,omitempty"`
	// TMDB is a snapshot of the title's TMDB page (rating, poster, banner,
	// cast...) set with /tmdb. nil = no TMDB link for this file.
	TMDB *TMDBInfo `bson:"tmdb,omitempty" json:"tmdb,omitempty"`
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
	contentRequests *mongo.Collection
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
		contentRequests: database.Collection("content_requests"),
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

// ContentRequest is a student's "please add this" request (movie, book
// chapter, whatever) — saved so the admin's Telegram notification can carry
// a "🔎 Find & Upload" button that looks it back up later by ID.
type ContentRequest struct {
	ID          string    `bson:"_id" json:"id"`
	Name        string    `bson:"name" json:"name"`
	ReleaseDate string    `bson:"release_date" json:"release_date"`
	DeviceID    string    `bson:"device_id" json:"device_id"`
	Status      string    `bson:"status" json:"status"` // pending | fulfilled
	CreatedAt   time.Time `bson:"created_at" json:"created_at"`
}

func (db *DB) createContentRequest(ctx context.Context, cr *ContentRequest) error {
	cr.CreatedAt = time.Now()
	cr.Status = "pending"
	_, err := db.contentRequests.InsertOne(ctx, cr)
	return err
}

func (db *DB) getContentRequest(ctx context.Context, id string) (*ContentRequest, error) {
	var cr ContentRequest
	err := db.contentRequests.FindOne(ctx, bson.M{"_id": id}).Decode(&cr)
	if err != nil {
		return nil, err
	}
	return &cr, nil
}

func (db *DB) markContentRequestFulfilled(ctx context.Context, id string) {
	db.contentRequests.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"status": "fulfilled"}})
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

// resetHomeIndex forces the home page to rebuild on the next request, so
// new uploads / deletes show up immediately instead of after the 45s TTL.
func resetHomeIndex() {
	homeIdxMu.Lock()
	homeIdx = nil
	homeIdxMu.Unlock()
}

func (db *DB) saveFile(ctx context.Context, f *FileRecord) error {
	defer resetHomeIndex()
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
	filter := bson.M{"file_name": primitive.Regex{Pattern: query, Options: "i"}, "hidden": bson.M{"$ne": true}}
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
	cursor, err := db.files.Find(ctx, bson.M{"hidden": bson.M{"$ne": true}}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	return out, cursor.All(ctx, &out)
}

func (db *DB) newestFiles(ctx context.Context, skip, limit int) ([]*FileRecord, error) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: 1}}).SetSkip(int64(skip)).SetLimit(int64(limit))
	cursor, err := db.files.Find(ctx, bson.M{"hidden": bson.M{"$ne": true}}, opts)
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
	cursor, err := db.files.Find(ctx, bson.M{"subject": subject, "hidden": bson.M{"$ne": true}}, opts)
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
	filter := bson.M{
		"hidden": bson.M{"$ne": true},
		"$or": []bson.M{
			{"year": year},
			{"file_name": bson.M{"$regex": yearRe}},
			{"description": bson.M{"$regex": yearRe}},
			{"subject": bson.M{"$regex": yearRe}},
			{"chapter": bson.M{"$regex": yearRe}},
			{"episode_label": bson.M{"$regex": yearRe}},
		},
	}
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
	defer resetHomeIndex()
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
	defer resetHomeIndex()
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
	defer resetHomeIndex()
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

// Referral-welcome video (set via /video2 on Telegram) — the file a brand
// new visitor lands on after clicking someone's referral link. Same
// pending-upload pattern; stores a file slug (not a URL), overriding
// cfg.ReferralWelcomeSlug with no expiry until /video2 off clears it.
func (c *Cache) setVideo2Pending(ctx context.Context, userID int64) {
	c.client.Set(ctx, fmt.Sprintf("video2:pending:%d", userID), "1", 5*time.Minute)
}
func (c *Cache) isVideo2Pending(ctx context.Context, userID int64) bool {
	v, err := c.client.Get(ctx, fmt.Sprintf("video2:pending:%d", userID)).Result()
	return err == nil && v == "1"
}
func (c *Cache) clearVideo2Pending(ctx context.Context, userID int64) {
	c.client.Del(ctx, fmt.Sprintf("video2:pending:%d", userID))
}
func (c *Cache) setReferralWelcomeSlug(ctx context.Context, slug string) {
	c.client.Set(ctx, "site:referralwelcome:slug", slug, 0)
}
func (c *Cache) getReferralWelcomeSlug(ctx context.Context) (string, bool) {
	slug, err := c.client.Get(ctx, "site:referralwelcome:slug").Result()
	return slug, err == nil && slug != ""
}
func (c *Cache) clearReferralWelcomeSlug(ctx context.Context) {
	c.client.Del(ctx, "site:referralwelcome:slug")
}

// Import source channel (set via /setsource) — the admin's other channel
// that /findreq searches when fulfilling a content request. Persisted with
// no expiry so it's a one-time setup.
func (c *Cache) setImportSource(ctx context.Context, channelID int64) {
	c.client.Set(ctx, "site:importsource:channel", strconv.FormatInt(channelID, 10), 0)
}
func (c *Cache) getImportSource(ctx context.Context) (int64, bool) {
	v, err := c.client.Get(ctx, "site:importsource:channel").Result()
	if err != nil || v == "" {
		return 0, false
	}
	id, pErr := strconv.ParseInt(v, 10, 64)
	return id, pErr == nil
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

// resolveChannelAccessHash looks up the access hash an MTProto channel
// peer needs — the same lookup getFileLocation does, pulled out so the
// search helper below can reuse it.
func resolveChannelAccessHash(ctx context.Context, api *tg.Client, channelID int64) (int64, error) {
	inputChan := &tg.InputChannel{ChannelID: channelID}
	result, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{inputChan})
	if err != nil {
		return 0, fmt.Errorf("get channel: %w", err)
	}
	if chats, ok := result.(*tg.MessagesChats); ok {
		for _, chat := range chats.Chats {
			if ch, ok := chat.(*tg.Channel); ok && ch.ID == channelID {
				return ch.AccessHash, nil
			}
		}
	}
	return 0, fmt.Errorf("channel not found or bot has no access")
}

// foundMessage is one hit from searchChannelByText — enough to show the
// admin a pick-list and to import the chosen one afterward.
type foundMessage struct {
	MessageID int
	Caption   string
	HasFile   bool
}

// searchChannelByText runs Telegram's native message search (messages.search)
// against one channel's history for the given query, and returns the
// messages that actually carry a document/video — this is what powers
// "🔎 Find & Upload": the admin names a request, this finds candidates in
// their other channel without them scrolling through it by hand. Requires
// MTProto (the bot's userbot session), not the plain Bot API, since the Bot
// API has no search-by-text capability over channel history.
func (p *MTProtoPool) searchChannelByText(ctx context.Context, channelID int64, query string, limit int) ([]foundMessage, error) {
	bot := p.next()
	if !bot.isReady() {
		return nil, fmt.Errorf("MTProto bot not ready")
	}
	bot.mu.Lock()
	api := bot.api
	bot.mu.Unlock()

	accessHash, err := resolveChannelAccessHash(ctx, api, channelID)
	if err != nil {
		return nil, err
	}

	result, err := api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
		Peer:   &tg.InputPeerChannel{ChannelID: channelID, AccessHash: accessHash},
		Q:      query,
		Filter: &tg.InputMessagesFilterEmpty{},
		Limit:  limit,
	})
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	var messages []tg.MessageClass
	switch m := result.(type) {
	case *tg.MessagesMessages:
		messages = m.Messages
	case *tg.MessagesMessagesSlice:
		messages = m.Messages
	case *tg.MessagesChannelMessages:
		messages = m.Messages
	}

	out := make([]foundMessage, 0, len(messages))
	for _, mc := range messages {
		m, ok := mc.(*tg.Message)
		if !ok {
			continue
		}
		_, hasDoc := m.Media.(*tg.MessageMediaDocument)
		out = append(out, foundMessage{MessageID: m.ID, Caption: m.Message, HasFile: hasDoc})
	}
	return out, nil
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
		a.pool.send(msg.Chat.ID, `📚 Commands (poori guide COMMANDS.md mein)

Sabke liye:
/start - Welcome
/help - Ye list
/setpass <file_id> <password|off> - Password (uploader/admin)

Upload / files (admin):
/u - Multi-quality upload shuru
/d - Multi-quality upload khatam, ek link
/delete <file_id> - File delete
/expire <file_id> <30m|12h|7d|1y|off> - Link expiry
/tag <file_id> <Subject> [/ Chapter] - Tag
/untag <file_id> - Tag hatao
/setyear <file_id> <year|0> - Year (1930-2030)
/setepisode <file_id> <label> - Season/Episode/Part
/dminem - Saari files delete (confirm puchta hai)

Bulk import (admin):
/setsource <channel_id> - Source channel set
/import <channel_id> <start> <end> - Bulk import (max 50000)
/cancelimport - Chal rahe import ko roko

Visitors (admin):
/user - Recent visitors
/profile <access_id> - Profile
/approve <access_id> - Approve
/block <access_id> - Block
/unblock <access_id> - Unblock
/reject <access_id> - Record delete
/deleteuser <access_id> - Approval + premium delete
/clearpending - Saare pending delete
/ban <user_id> - Telegram user ban
/unban <user_id> - Unban

Premium (admin):
/premium <access_id> <days> - Premium do
/unpremium <access_id> - Premium hatao
/gencode <time> [kitne_log] - Code banao
/deletecode <code> - Code delete

Website (admin):
/reply <msg> - Site announcement (/clearreply se hatao)
/clearreply - Announcement hatao
/code <html/css/js> - Custom section (/removecode se hatao)
/removecode - Custom section hatao
/video [url|off] - Password page video
/video2 [off] - Referral welcome video
/advertise [url|off] - Watch page ad banner
/adlink <url> [button text] - Ad/sponsor link (banner clickable + Sponsored button), /adlink off
/tmdb <file_id> <tmdb_link> - TMDB se rating, poster, banner, cast sab laao (/tmdb <file_id> off)
/autotmdb - Purani files (bina TMDB wali) ko file name se auto-match karo (nayi uploads mein ye khud hota hai)
/admin <naam> <image_url> - Team badge
/removeadmin <naam> - Team badge hatao
/dashboard - Admin dashboard link
/stats - Statistics

Koi bhi file bhejo, permanent link mil jayega. Links by default permanent hain; /expire se expiry lagao.`)
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
		primary := preferredQualityFile(files)
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
	case "gencode":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		gcParts := strings.Fields(msg.CommandArguments())
		usage := "Usage: /gencode <time> [kitne_log]\n\n<time>: sirf number = din (30), ya 90m / 12h / 7d / 1y\n[kitne_log]: kitne alag devices is code ko use kar sakte hain (default 1)\n\nJaise:\n/gencode 30 — 30 din, 1 banda\n/gencode 12h — 12 ghante, 1 banda\n/gencode 7d 50 — 7 din, 50 log tak\n/gencode 30m 5 — 30 minute, 5 log tak"
		if len(gcParts) < 1 || len(gcParts) > 2 {
			a.pool.send(msg.Chat.ID, usage)
			return
		}
		days, minutes := 0, 0
		if n, nErr := strconv.Atoi(gcParts[0]); nErr == nil {
			if n <= 0 {
				a.pool.send(msg.Chat.ID, usage)
				return
			}
			days = n
		} else {
			dur, clr, dErr := parseExpiryDuration(gcParts[0])
			if dErr != nil || clr || dur < time.Minute {
				a.pool.send(msg.Chat.ID, usage)
				return
			}
			minutes = int(dur / time.Minute)
		}
		maxUses := 1
		if len(gcParts) == 2 {
			n, nErr := strconv.Atoi(gcParts[1])
			if nErr != nil || n < 1 || n > 100000 {
				a.pool.send(msg.Chat.ID, "❌ Kitne log: 1 se 100000 ke beech ek number do.\n\n"+usage)
				return
			}
			maxUses = n
		}
		validity := fmt.Sprintf("%d din", days)
		if minutes > 0 {
			switch {
			case minutes%1440 == 0:
				validity = fmt.Sprintf("%d din", minutes/1440)
			case minutes%60 == 0:
				validity = fmt.Sprintf("%d ghante", minutes/60)
			default:
				validity = fmt.Sprintf("%d minute", minutes)
			}
		}
		// Retry a couple of times on the astronomically unlikely chance of
		// a code collision, so this never silently fails.
		var code string
		var createErr error
		for attempt := 0; attempt < 3; attempt++ {
			code = strings.ToUpper(strings.ReplaceAll(uuid.New().String()[:8], "-", ""))
			if createErr = a.db.createPremiumCodeAdv(ctx, code, days, minutes, maxUses, msg.From.ID); createErr == nil {
				break
			}
		}
		if createErr != nil {
			a.logger.Warn("gencode: createPremiumCode failed", zap.Error(createErr))
			a.pool.send(msg.Chat.ID, "❌ Code generate nahi ho paya (database error), dobara try karo.")
			return
		}
		base := a.cfg.baseURL()
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"✅ *Premium code ready\\!*\n\n🎟️ Code: `%s`\n⏳ Validity: %s\n👥 Kitne log use kar sakte hain: %d\n\nCustomer ko yeh code bhejo — unhe %s/redeem pe jaake \"Have a Code?\" section mein daalne ko bolo\\. Har device sirf ek baar use kar sakta hai, aur %d log hone ke baad code kaam nahi karega\\.\n\nHatana ho to: /deletecode %s",
			code, mdEscape(validity), maxUses, mdEscape(base), maxUses, code,
		))
	case "deletecode":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		code := strings.ToUpper(strings.TrimSpace(msg.CommandArguments()))
		if code == "" {
			a.pool.send(msg.Chat.ID, "Usage: /deletecode <code>")
			return
		}
		found, err := a.db.deletePremiumCode(ctx, code)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Delete failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Code `%s` nahi mila (galat hai ya already delete ho chuka hai).", code))
			return
		}
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf("✅ Code `%s` delete ho gaya\\.", code))
	case "deleteuser":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		var accessID int
		if _, err := fmt.Sscanf(strings.TrimSpace(msg.CommandArguments()), "%d", &accessID); err != nil {
			a.pool.send(msg.Chat.ID, "Usage: /deleteuser <access_id>\n\nUnki approval/premium record poori tarah delete kar deta hai — unhe dobara se pura request/approval process karna padega.")
			return
		}
		approval, aErr := a.db.getApprovalByAccessID(ctx, accessID)
		if aErr != nil || approval == nil {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		found, err := a.db.deleteByAccessID(ctx, accessID)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Delete failed, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Access ID `%05d` nahi mila.", accessID))
			return
		}
		a.db.revokePremium(ctx, approval.DeviceID)
		a.pool.sendMD(msg.Chat.ID, fmt.Sprintf(
			"✅ Access ID `%05d` poori tarah delete ho gaya \\(approval \\+ premium dono hata diye\\)\\. Unhe ab dobara se login/approval process karna hoga\\.",
			accessID,
		))
	case "admin":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		parts := strings.Fields(msg.CommandArguments())
		if len(parts) < 2 {
			a.pool.send(msg.Chat.ID, "Usage: /admin <name> <image_url>\n\nJaise: /admin Rahul https://example.com/photo.jpg\n\nYeh sirf website pe naam + photo ke saath 'Admin' badge dikhata hai — koi real bot-access nahi milta, sirf showcase/credit ke liye hai. Owner hamesha tum hi rahoge.\n\nHatana ho to: /removeadmin <name>")
			return
		}
		imageURL := parts[len(parts)-1]
		name := strings.TrimSpace(strings.TrimSuffix(msg.CommandArguments(), imageURL))
		if name == "" || imageURL == "" {
			a.pool.send(msg.Chat.ID, "Usage: /admin <name> <image_url>")
			return
		}
		if err := a.db.addTeamMember(ctx, name, imageURL); err != nil {
			a.pool.send(msg.Chat.ID, "❌ Add nahi ho paya, dobara try karo.")
			return
		}
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ %s ko website pe 'Admin' badge ke saath add kar diya. Website pe Settings mein dikhega.", name))
	case "removeadmin":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		name := strings.TrimSpace(msg.CommandArguments())
		if name == "" {
			a.pool.send(msg.Chat.ID, "Usage: /removeadmin <name>")
			return
		}
		found, err := a.db.removeTeamMember(ctx, name)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Remove nahi ho paya, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ '%s' naam se koi entry nahi mili.", name))
			return
		}
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ %s ko website se hata diya.", name))
	case "premium":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		parts := strings.Fields(msg.CommandArguments())
		if len(parts) != 2 {
			a.pool.send(msg.Chat.ID, "Usage: /premium <access_id ya device_id> <days>\n\nJaise: /premium 48213 2")
			return
		}
		deviceID, okRef := a.resolveDeviceRef(ctx, parts[0])
		if !okRef {
			a.pool.send(msg.Chat.ID, "❌ Yeh Access ID nahi mila.")
			return
		}
		days, convErr := strconv.Atoi(parts[1])
		if convErr != nil || days <= 0 {
			a.pool.send(msg.Chat.ID, "❌ Days ek positive number honi chahiye.")
			return
		}
		expiresAt, gErr := a.db.grantPremiumDirect(ctx, deviceID, days)
		if gErr != nil {
			a.pool.send(msg.Chat.ID, "❌ Premium grant nahi ho paya, dobara try karo.")
			return
		}
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ Premium granted — expires %s.", expiresAt.Format("02 Jan 2006, 15:04 MST")))
	case "unpremium":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		ref := strings.TrimSpace(msg.CommandArguments())
		if ref == "" {
			a.pool.send(msg.Chat.ID, "Usage: /unpremium <access_id ya device_id>")
			return
		}
		deviceID, okRef := a.resolveDeviceRef(ctx, ref)
		if !okRef {
			a.pool.send(msg.Chat.ID, "❌ Yeh Access ID nahi mila.")
			return
		}
		found, rErr := a.db.revokePremium(ctx, deviceID)
		if rErr != nil {
			a.pool.send(msg.Chat.ID, "❌ Error, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "ℹ️ Yeh device premium nahi tha.")
			return
		}
		a.pool.send(msg.Chat.ID, "✅ Premium revoke kar diya.")
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
	case "video":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		varg := strings.TrimSpace(msg.CommandArguments())
		switch {
		case strings.EqualFold(varg, "off"):
			a.cache.clearPromptVideo(ctx)
			a.pool.send(msg.Chat.ID, "✅ Custom video hata diya gaya — password/approval page ab bina video ke (ya PASSWORD_PROMPT_VIDEO_URL wale env var pe) dikhega.")
		case varg == "":
			a.cache.setPromptVideoPending(ctx, msg.From.ID)
			a.pool.send(msg.Chat.ID,
				"🎥 Theek hai — ab agla video jo tum bhejoge (5 min ke andar), wahi password/approval page (jahan Access ID dikhta hai) ke upar chalega.\n\nYa seedha URL bhi de sakte ho:\n/video <video ka URL>\n\nHatane ke liye: /video off")
		default:
			a.cache.setPromptVideo(ctx, varg)
			a.pool.send(msg.Chat.ID, "✅ Video URL set ho gaya — password/approval page pe ab yeh chalega.")
		}
	case "video2":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		v2arg := strings.TrimSpace(msg.CommandArguments())
		switch {
		case strings.EqualFold(v2arg, "off"):
			a.cache.clearReferralWelcomeSlug(ctx)
			a.pool.send(msg.Chat.ID, "✅ Referral welcome video revert ho gaya default/env wale pe — file khud delete nahi hui, sirf link hata hai.")
		case v2arg == "":
			a.cache.setVideo2Pending(ctx, msg.From.ID)
			a.pool.send(msg.Chat.ID,
				"🎥 Theek hai — ab agla video jo tum bhejoge (5 min ke andar), wahi ab se naye referral users ko sabse pehle dikhega (jab koi kisi ki referral link se aayega).\n\nHatane ke liye: /video2 off")
		default:
			a.pool.send(msg.Chat.ID, "❌ /video2 sirf ek video upload accept karta hai (URL se nahi), kyunki yeh ek poora watchable file banta hai (apna slug, apna /watch page). Sirf /video2 bhejo, phir video attach karo.")
		}
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
	case "adlink":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		linkArg := strings.TrimSpace(msg.CommandArguments())
		switch {
		case linkArg == "":
			a.pool.send(msg.Chat.ID,
				"Usage: /adlink <url> [button ka text]\n\nExample:\n/adlink https://example.com Visit Sponsor\n\nIsse (1) advertise banner clickable ho jaata hai aur (2) player ke neeche ek \"Sponsored\" button dikhta hai.\n\nHatane ke liye: /adlink off")
		case strings.EqualFold(linkArg, "off"):
			a.cache.clearAdLink(ctx)
			a.pool.send(msg.Chat.ID, "✅ Ad link hata diya gaya.")
		default:
			lp := strings.SplitN(linkArg, " ", 2)
			linkURL := strings.TrimSpace(lp[0])
			lowerLink := strings.ToLower(linkURL)
			if !strings.HasPrefix(lowerLink, "http://") && !strings.HasPrefix(lowerLink, "https://") {
				a.pool.send(msg.Chat.ID, "❌ Link http:// ya https:// se shuru hona chahiye.")
				return
			}
			linkText := ""
			if len(lp) > 1 {
				linkText = strings.TrimSpace(lp[1])
			}
			if linkText == "" {
				linkText = "Visit Sponsor"
			}
			if rs := []rune(linkText); len(rs) > 40 {
				linkText = string(rs[:40])
			}
			if err := a.cache.setAdLink(ctx, &adLinkSettings{URL: linkURL, Text: linkText}); err != nil {
				a.pool.send(msg.Chat.ID, "❌ Ad link set nahi ho paya, dobara try karo.")
				return
			}
			a.pool.send(msg.Chat.ID, "✅ Ad link set ho gaya — watch page pe banner clickable hai aur \""+linkText+"\" button player ke neeche dikhega.")
		}
	case "tmdb":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		tArgs := strings.TrimSpace(msg.CommandArguments())
		tParts := strings.SplitN(tArgs, " ", 2)
		if len(tParts) < 2 || strings.TrimSpace(tParts[1]) == "" {
			a.pool.send(msg.Chat.ID,
				"Usage: /tmdb <file_id> <tmdb_link>\n\nExample:\n/tmdb abc123 https://www.themoviedb.org/movie/603-the-matrix\n/tmdb abc123 https://www.themoviedb.org/tv/1396-breaking-bad\n\nHatane ke liye: /tmdb <file_id> off\n\nMulti-quality (/u .. /d) wali file mein saari qualities ko ek saath mil jaata hai.")
			return
		}
		tFileID := tParts[0]
		tRest := strings.TrimSpace(tParts[1])
		if strings.EqualFold(tRest, "off") {
			found, err := a.db.setFileTMDB(ctx, tFileID, nil)
			if err != nil {
				a.pool.send(msg.Chat.ID, "❌ TMDB info hatane mein dikkat aayi, dobara try karo.")
				return
			}
			if !found {
				a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
				return
			}
			a.pool.send(msg.Chat.ID, "✅ TMDB info hata di.")
			return
		}
		if a.cfg.TMDBAPIKey == "" {
			a.pool.send(msg.Chat.ID, "❌ TMDB_API_KEY environment variable set nahi hai. themoviedb.org/settings/api se free key lo aur Render/server ke env mein TMDB_API_KEY naam se daalo.")
			return
		}
		tType, tID, tOK := parseTMDBLink(tRest)
		if !tOK {
			a.pool.send(msg.Chat.ID, "❌ Yeh TMDB link nahi lag raha. Aisa link do: https://www.themoviedb.org/movie/603 ya https://www.themoviedb.org/tv/1396")
			return
		}
		tctx, tcancel := context.WithTimeout(ctx, 15*time.Second)
		info, err := tmdbFetch(tctx, a.cfg.TMDBAPIKey, tType, tID)
		tcancel()
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ TMDB se data nahi aaya: "+err.Error())
			return
		}
		found, err := a.db.setFileTMDB(ctx, tFileID, info)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ Save nahi ho paya, dobara try karo.")
			return
		}
		if !found {
			a.pool.send(msg.Chat.ID, "❌ File ID nahi mila.")
			return
		}
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ TMDB linked!\n\n🎬 %s (%s)\n⭐ %.1f/10 (%d votes)\n🎭 %s\n👥 %d cast, banner + poster mil gaye.\n\nAb watch page aur cards mein sab dikhega.",
			info.Title, info.Year, info.Rating, info.Votes, strings.Join(info.Genres, ", "), len(info.Cast)))
	case "autotmdb":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		if a.cfg.TMDBAPIKey == "" {
			a.pool.send(msg.Chat.ID, "❌ TMDB_API_KEY environment variable set nahi hai.")
			return
		}
		a.pool.send(msg.Chat.ID, "⏳ Purani files (jin mein TMDB info nahi hai) ke liye TMDB match dhoondh raha hun — ek baar mein max 100 files. Khatam hone par report bhejunga.")
		go a.runAutoTMDBBackfill(100)
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
	case "reply":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		text := strings.TrimSpace(msg.CommandArguments())
		if text == "" {
			a.pool.send(msg.Chat.ID, "Usage: /reply <message>\n\nYeh message website pe SABKO ek glowing banner mein dikhega (jaise cinema announcement) — kisi specific visitor ko target karne ki zaroorat nahi.\n\nHatane ke liye: /clearreply")
			return
		}
		a.cache.setSiteAnnouncement(ctx, text)
		a.pool.send(msg.Chat.ID, "✅ Announcement set ho gaya — website pe sabko glowing banner mein dikhega.")
	case "code":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		// Raw content after "/code " — not TrimSpace-normalized beyond the ends,
		// so indentation/newlines inside the HTML/CSS/JS are kept exactly as
		// typed. This is admin-only trusted input and is rendered as-is (with
		// <script> tags executed) in a section at the bottom of every page.
		codeText := msg.CommandArguments()
		codeText = strings.Trim(codeText, "\r\n")
		if strings.TrimSpace(codeText) == "" {
			a.pool.send(msg.Chat.ID, "Usage: /code <html/css/js>\n\nJo bhi bhejoge — HTML, CSS (<style> mein), JS (<script> mein), ya sab mix — wahi website ke sabse niche, footer ke neeche ek section mein sabko dikhega. <script> tags bhi chalte hain.\n\nJaise:\n/code <div style=\"color:red\">Hello</div><script>console.log('hi')</script>\n\nHatane ke liye: /removecode")
			return
		}
		a.cache.setCustomSection(ctx, codeText)
		a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ Custom section set ho gaya (%d characters) — website pe sabse niche har page pe dikhega. Visitors ko page reload/refresh karna padega.\n\nHatane ke liye: /removecode", len(codeText)))
	case "removecode":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		a.cache.clearCustomSection(ctx)
		a.pool.send(msg.Chat.ID, "✅ Custom section hata diya gaya.")

	case "clearreply":
		if msg.From.ID != a.cfg.AdminID {
			a.pool.send(msg.Chat.ID, "❌ Admin only.")
			return
		}
		a.cache.clearSiteAnnouncement(ctx)
		a.pool.send(msg.Chat.ID, "✅ Announcement hata diya gaya.")
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
	case "setsource":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		scArg := strings.TrimSpace(msg.CommandArguments())
		scID, scErr := strconv.ParseInt(scArg, 10, 64)
		if scErr != nil {
			a.pool.send(msg.Chat.ID, "Usage: /setsource <channel_id>\n\nTumhara doosra channel jahan videos/PDFs pehle se hain. ID @userinfobot ya @RawDataBot ko koi post forward karke milti hai.\n\nEk baar set karne ke baad, har Movie/Book Request ke saath admin ko ek \"Find & Upload\" button milega jo isi channel mein dhoondhega.")
			return
		}
		a.cache.setImportSource(ctx, scID)
		a.pool.send(msg.Chat.ID, "✅ Source channel set ho gaya. Ab har naye request ke saath 🔎 Find & Upload button aayega.")
	case "import":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		iparts := strings.Fields(msg.CommandArguments())
		usage := "Usage: /import <source_channel_id> <start_msg_id> <end_msg_id>\n\n" +
			"Tumhara doosra channel jahan videos pehle se hain, uski channel ID chahiye (jaise -1001234567890) — " +
			"kisi bhi post ko @userinfobot ya @RawDataBot ko forward karke mil jaati hai.\n\n" +
			"start/end = us channel ke message ID range jitne import karne hain (channel ke messages ऊपर se neeche sequential IDs mein hote hain).\n\n" +
			"Is bot ko us channel mein admin hona chahiye (ya kam se kam member), warna forward fail hoga.\n\n" +
			"Jaise: /import -1001234567890 50 300\n\n" +
			"Ek baar mein max 50000 messages. Progress percentage mein dikhega, rokne ke liye /cancelimport."
		if len(iparts) != 3 {
			a.pool.send(msg.Chat.ID, usage)
			return
		}
		srcChannelID, scErr := strconv.ParseInt(iparts[0], 10, 64)
		startID, sErr := strconv.Atoi(iparts[1])
		endID, eErr := strconv.Atoi(iparts[2])
		if scErr != nil || sErr != nil || eErr != nil || startID < 1 || endID < startID {
			a.pool.send(msg.Chat.ID, "❌ Galat input.\n\n"+usage)
			return
		}
		if endID-startID+1 > importMaxRange {
			a.pool.send(msg.Chat.ID, fmt.Sprintf("❌ Ek baar mein max %d messages. Tumne %d maange hain — range chhoti karo.", importMaxRange, endID-startID+1))
			return
		}
		if !importRunning.CompareAndSwap(false, true) {
			a.pool.send(msg.Chat.ID, "⚠️ Ek import pehle se chal raha hai. Rokne ke liye /cancelimport bhejo.")
			return
		}
		importCancel.Store(false)
		n := endID - startID + 1
		a.pool.send(msg.Chat.ID, fmt.Sprintf(
			"🚚 Import shuru: message %d se %d tak (%d total). Andaza: ~%s. Background mein chalega, ek hi status message percentage ke saath update hota rahega. Rokne ke liye /cancelimport.",
			startID, endID, n, importFmtDur(time.Duration(n)*importDelay)))
		go a.runImportJob(context.Background(), msg.Chat.ID, srcChannelID, startID, endID)
	case "cancelimport":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		if !importRunning.Load() {
			a.pool.send(msg.Chat.ID, "ℹ️ Abhi koi import nahi chal raha.")
			return
		}
		importCancel.Store(true)
		a.pool.send(msg.Chat.ID, "🛑 Cancel request mil gayi — current message ke baad import ruk jayega.")
	case "delete":
		if msg.From.ID != a.cfg.AdminID {
			return
		}
		slug := strings.TrimSpace(msg.CommandArguments())
		if slug == "" {
			a.pool.send(msg.Chat.ID, "Usage: /delete <file_id>\n\nAgar file multi-quality group ka hissa hai, saari qualities ek saath delete ho jaayengi.")
			return
		}
		rec, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			a.pool.send(msg.Chat.ID, "❌ File not found.")
			return
		}

		// If the file being deleted is currently wired up as the password-prompt
		// video or the referral-welcome video, clear that override too — a
		// deleted file left referenced there is exactly the "black screen"
		// bug we fixed earlier, just triggered a different way.
		if promoURL, ok := a.cache.getPromptVideo(ctx); ok && strings.HasSuffix(promoURL, "/"+slug) {
			a.cache.clearPromptVideo(ctx)
		}
		if refSlug, ok := a.cache.getReferralWelcomeSlug(ctx); ok && refSlug == slug {
			a.cache.clearReferralWelcomeSlug(ctx)
		}

		// If this file belongs to a multi-quality group (/u ... /d), delete
		// every quality variant together — from the visitor's point of view
		// (and everywhere in the UI: New Releases, Trending, the quality
		// switcher) it's presented as ONE movie/file, so deleting just one
		// quality left the others still visible and playable, which looked
		// like delete "didn't work".
		if rec.GroupID != nil && *rec.GroupID != "" {
			members, mErr := a.db.listGroupFiles(ctx, *rec.GroupID)
			if mErr != nil {
				a.pool.send(msg.Chat.ID, "❌ Delete failed — DB error, dobara try karo.")
				return
			}
			deletedCount := 0
			for _, m := range members {
				found, dErr := a.db.deleteFileByID(ctx, m.ID)
				if dErr != nil {
					a.logger.Warn("delete: group member delete failed", zap.Error(dErr), zap.String("id", m.ID))
					continue
				}
				if found {
					deletedCount++
					a.cache.delFile(ctx, m.ID)
				}
			}
			if deletedCount == 0 {
				a.pool.send(msg.Chat.ID, "❌ Delete failed — koi bhi quality variant delete nahi ho paya, dobara try karo.")
				return
			}
			a.pool.send(msg.Chat.ID, fmt.Sprintf("✅ File deleted — %d quality variant(s) ek saath hataayi gayi.", deletedCount))
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

func (a *App) storeAdvertiseMedia(ctx context.Context, msg *tgbotapi.Message) (streamURL, adType, slugOut string, err error) {
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
		return "", "", "", fmt.Errorf("unsupported media for advertise")
	}

	fwdMsg, err := a.pool.next().Send(tgbotapi.NewForward(a.cfg.DBChannelID, msg.Chat.ID, msg.MessageID))
	if err != nil {
		return "", "", "", fmt.Errorf("forward failed: %w", err)
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
		Hidden:       true, // never show ad/prompt/referral-welcome media in public browsing/search
	}
	if err := a.db.saveFile(ctx, rec); err != nil {
		return "", "", "", fmt.Errorf("db save failed: %w", err)
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
	return streamURL, adType, slug, nil
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


// importSingleMessage forwards one message from srcChannelID into the DB
// channel and saves it exactly like a normal upload (extractFileInfo +
// saveFile + cache.setFile). Shared by /import's range loop and the
// "🔎 Find & Upload" request-fulfillment button — the only two ways a
// message ever gets pulled in from the admin's other channel, and both
// require the admin to have explicitly pointed at this exact message.
func (a *App) importSingleMessage(ctx context.Context, srcChannelID int64, msgID int, uploaderID int64) (*FileRecord, error) {
	fwdMsg, err := a.pool.next().Send(tgbotapi.NewForward(a.cfg.DBChannelID, srcChannelID, msgID))
	if err != nil {
		return nil, fmt.Errorf("forward failed: %w", err)
	}
	fi, ok := extractFileInfo(&fwdMsg)
	if !ok {
		return nil, fmt.Errorf("message %d has no file", msgID)
	}
	hash := makeShortHash(fi.fileName, fi.fileSize, fwdMsg.MessageID)
	slug := uuid.New().String()
	channelID := toInternalChannelID(a.cfg.DBChannelID)
	rec := &FileRecord{
		ID: slug, MessageID: fwdMsg.MessageID, ChannelID: channelID,
		FileName: fi.fileName, FileSize: fi.fileSize, MimeType: fi.mimeType,
		Hash: hash, UploaderID: uploaderID, UploaderName: "imported",
	}
	if err := a.db.saveFile(ctx, rec); err != nil {
		return nil, fmt.Errorf("db save failed: %w", err)
	}
	a.cache.setFile(ctx, slug, &cachedFile{
		MessageID: fwdMsg.MessageID, ChannelID: channelID,
		FileName: fi.fileName, FileSize: fi.fileSize, MimeType: fi.mimeType, Hash: hash,
	})
	a.autoTMDBMode(rec, true) // background TMDB match (banner, rating...), quiet for bulk imports
	return rec, nil
}

// runImportJob bulk-migrates a message-ID range from the admin's own other
// channel into this bot's library, one message at a time, via
// importSingleMessage. Runs in its own goroutine with a deliberate delay
// between messages to stay well under Telegram's forwarding rate limits;
// skips anything that isn't a file (text posts, already-deleted messages)
// and keeps going.
// importMaxRange is the most messages a single /import may cover.
const importMaxRange = 50000

// importDelay is the pause between forwards, kept to stay under Telegram's
// forwarding rate limits.
const importDelay = 1200 * time.Millisecond

// Only one import runs at a time; /cancelimport flips importCancel.
var (
	importRunning atomic.Bool
	importCancel  atomic.Bool
)

func importBar(pct float64) string {
	const width = 20
	filled := int(pct / 100 * width)
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func importFmtDur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	sec := int(d.Seconds()) % 60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	case m > 0:
		return fmt.Sprintf("%dm %ds", m, sec)
	default:
		return fmt.Sprintf("%ds", sec)
	}
}

// importRetryAfter extracts N from Telegram's "Too Many Requests: retry after N".
func importRetryAfter(err error) int {
	const key = "retry after "
	s := err.Error()
	i := strings.Index(s, key)
	if i < 0 {
		return 0
	}
	n := 0
	fmt.Sscanf(s[i+len(key):], "%d", &n)
	return n
}

// runImportJob bulk-migrates a message-ID range from the admin's own other
// channel into this bot's library, one message at a time, via
// importSingleMessage. Runs in its own goroutine with a deliberate delay
// between messages to stay well under Telegram's forwarding rate limits;
// skips anything that isn't a file (text posts, already-deleted messages)
// and keeps going. Progress is shown as a percentage on ONE status message
// that is edited in place (no chat spam), and Telegram flood-waits are
// honoured automatically. /cancelimport stops it cleanly.
func (a *App) runImportJob(ctx context.Context, adminChatID int64, srcChannelID int64, startID, endID int) {
	defer importRunning.Store(false)

	imported, skipped, failed := 0, 0, 0
	total := endID - startID + 1
	began := time.Now()
	lastMsgID := startID - 1
	lastErr := ""
	cancelled := false

	render := func(done int, title string) string {
		pct := float64(done) * 100 / float64(total)
		elapsed := time.Since(began)
		eta := "—"
		if done > 0 && done < total {
			eta = importFmtDur(time.Duration(float64(elapsed) / float64(done) * float64(total-done)))
		}
		return fmt.Sprintf(
			"%s\n\n[%s] %.1f%%\n\n📊 Scan: %d / %d\n✅ Imported: %d\n⏭️ Skip (file nahi thi): %d\n❌ Fail: %d\n📍 Last msg ID: %d\n\n⏱ Beeta: %s • Baaki ≈ %s",
			title, importBar(pct), pct, done, total, imported, skipped, failed, lastMsgID,
			importFmtDur(elapsed), eta)
	}

	statusID := 0
	if sm, err := a.pool.primary().Send(tgbotapi.NewMessage(adminChatID,
		render(0, "🚚 Import chal raha hai (rokne ke liye /cancelimport)"))); err == nil {
		statusID = sm.MessageID
	}
	lastEdit := time.Now()
	editStatus := func(text string) {
		if statusID == 0 {
			return
		}
		// "message is not modified" and similar errors are harmless.
		a.pool.primary().Send(tgbotapi.NewEditMessageText(adminChatID, statusID, text))
	}

	for msgID := startID; msgID <= endID; msgID++ {
		if importCancel.Load() {
			cancelled = true
			break
		}

		var err error
		for attempt := 0; attempt < 5; attempt++ {
			_, err = a.importSingleMessage(ctx, srcChannelID, msgID, adminChatID)
			if err == nil {
				break
			}
			if wait := importRetryAfter(err); wait > 0 {
				time.Sleep(time.Duration(wait+1) * time.Second)
				continue
			}
			break
		}

		switch {
		case err == nil:
			imported++
		case strings.Contains(err.Error(), "no file"):
			skipped++
		default:
			failed++
			lastErr = err.Error()
		}
		lastMsgID = msgID

		if time.Since(lastEdit) >= 8*time.Second {
			editStatus(render(imported+skipped+failed, "🚚 Import chal raha hai (rokne ke liye /cancelimport)"))
			lastEdit = time.Now()
		}

		time.Sleep(importDelay)
	}

	done := imported + skipped + failed
	title := "✅ Import khatam!"
	if cancelled {
		title = "🛑 Import rok diya gaya"
	}
	editStatus(render(done, title))

	summary := fmt.Sprintf(
		"%s\n\n📥 Imported: %d\n⏭️ Skip (file nahi thi): %d\n❌ Fail (forward error): %d\n",
		title, imported, skipped, failed)
	if lastErr != "" {
		summary += "\nLast error: " + lastErr + "\n"
	}
	if cancelled && lastMsgID < endID {
		summary += fmt.Sprintf("\n▶️ Aage badhane ke liye:\n/import %d %d %d\n", srcChannelID, lastMsgID+1, endID)
	}
	if a.cfg.TMDBAPIKey != "" {
		summary += "\n🎬 TMDB banner/rating import ki files par background mein khud lag rahe hain (thodi der lagegi). Jin ka match nahi milega, unke liye baad mein /autotmdb chalao — wo list bhi dega.\n"
	}
	summary += "\nSaari nayi files homepage pe normal tarike se dikhengi — unko /setpass, /setyear ya quality-tag abhi bhi manually lagana padega agar chahiye."
	a.pool.send(adminChatID, summary)
}

func (a *App) onFile(ctx context.Context, msg *tgbotapi.Message) {
	if msg.From.ID == a.cfg.AdminID && a.cache.isPromptVideoPending(ctx, msg.From.ID) && msg.Video != nil {
		a.cache.clearPromptVideoPending(ctx, msg.From.ID)
		_, _, slug, err := a.storeAdvertiseMedia(ctx, msg)
		if err != nil {
			a.logger.Error("prompt video store failed", zap.Error(err))
			a.pool.send(msg.Chat.ID, "❌ Video store nahi ho paya, dobara try karo.")
			return
		}
		// /promo/ (not /stream/) — this must play on the password-entry
		// page itself, before the visitor has unlocked anything, so it
		// can't go through the password/approval-gated stream endpoint.
		promoURL := fmt.Sprintf("%s/promo/%s", a.cfg.baseURL(), slug)
		a.cache.setPromptVideo(ctx, promoURL)
		a.pool.send(msg.Chat.ID, fmt.Sprintf(
			"✅ Video set ho gaya — password/approval page pe ab yeh chalega, koi login/password ki zaroorat nahi. Homepage ki scroll list mein yeh nahi dikhega.\n\n🔑 Unique code: %s\nIsse hatana ho to: /delete %s (ya /video off karke sirf link revert karo, file safe rahegi)\n\nTest karne ke liye ek nayi link kholo (purani tab cache ki wajah se purana dikha sakti hai, ek baar reload kar lena).",
			slug, slug))
		return
	}

	if msg.From.ID == a.cfg.AdminID && a.cache.isVideo2Pending(ctx, msg.From.ID) && msg.Video != nil {
		a.cache.clearVideo2Pending(ctx, msg.From.ID)
		_, _, slug, err := a.storeAdvertiseMedia(ctx, msg)
		if err != nil {
			a.logger.Error("video2 store failed", zap.Error(err))
			a.pool.send(msg.Chat.ID, "❌ Video store nahi ho paya, dobara try karo.")
			return
		}
		a.cache.setReferralWelcomeSlug(ctx, slug)
		a.pool.send(msg.Chat.ID, fmt.Sprintf(
			"✅ Video set ho gaya — koi bhi naya user jo referral link se aayega, ab yeh video sabse pehle dekhega.\n\n🔑 Unique code: %s\nIsse hatana ho to: /delete %s (ya /video2 off karke sirf link revert karo, file safe rahegi)\n\nHomepage ki list mein yeh nahi dikhega.",
			slug, slug))
		return
	}

	if msg.From.ID == a.cfg.AdminID && a.cache.isAdvertisePending(ctx, msg.From.ID) &&
		(msg.Video != nil || len(msg.Photo) > 0) {
		a.cache.clearAdvertisePending(ctx, msg.From.ID)
		streamURL, adType, _, err := a.storeAdvertiseMedia(ctx, msg)
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
	a.autoTMDB(rec) // background: find + link TMDB info by file name

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
	a.autoTMDB(rec) // background: find + link TMDB info (once per quality group)

	a.cache.setFile(ctx, slug, &cachedFile{
		MessageID: fwdMsg.MessageID, ChannelID: channelID,
		FileName: fi.fileName, FileSize: fi.fileSize, MimeType: fi.mimeType, Hash: hash,
		GroupID: &gid, QualityLabel: label, QualityRank: rank,
	})
	a.cache.addMultiUploadEntry(ctx, groupID, fmt.Sprintf("%s|%s", label, fi.fileName))
	a.cache.refreshMultiUploadSession(ctx, msg.From.ID)

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

// handleFindRequest is the "🔎 Find & Upload" button's logic: look up the
// saved request, search the admin's configured source channel for its name,
// and either import the one clear match automatically, ask the admin to
// pick when there's more than one, or say so when there's none. Either way
// this only ever runs because the admin tapped a button on one specific
// request — it never fires on its own.
func (a *App) handleFindRequest(ctx context.Context, chatID int64, msgID int, reqID string) {
	req, err := a.db.getContentRequest(ctx, reqID)
	if err != nil {
		a.pool.send(chatID, "❌ Request not found (shayad purana hai).")
		return
	}
	srcChannelID, ok := a.cache.getImportSource(ctx)
	if !ok {
		a.pool.send(chatID, "❌ Pehle /setsource <channel_id> se apna doosra channel set karo.")
		return
	}
	hits, err := a.mtPool.searchChannelByText(ctx, srcChannelID, req.Name, 10)
	if err != nil {
		a.logger.Error("searchChannelByText failed", zap.Error(err))
		a.pool.send(chatID, "❌ Search fail ho gaya (MTProto error) — /import se manually try karo.")
		return
	}
	var withFile []foundMessage
	for _, h := range hits {
		if h.HasFile {
			withFile = append(withFile, h)
		}
	}

	switch len(withFile) {
	case 0:
		a.pool.send(chatID, fmt.Sprintf("❌ \"%s\" se match karti koi file nahi mili us channel mein. Naam alag ho sakta hai — /import se message ID range daal ke manually try karo.", req.Name))
	case 1:
		rec, err := a.importSingleMessage(ctx, srcChannelID, withFile[0].MessageID, a.cfg.AdminID)
		if err != nil {
			a.pool.send(chatID, "❌ Mil gayi thi lekin upload fail ho gaya: "+err.Error())
			return
		}
		a.db.markContentRequestFulfilled(ctx, reqID)
		base := a.cfg.baseURL()
		a.pool.sendMD(chatID, fmt.Sprintf(
			"✅ *Mil gayi aur upload ho gayi\\!*\n\n📄 `%s`\n🆔 `%s`\n📺 [Watch](%s/watch/%s)",
			mdEscape(rec.FileName), rec.ID, base, rec.ID,
		))
	default:
		// More than one candidate — don't guess which one is right, let
		// the admin pick with one more tap.
		var rows [][]tgbotapi.InlineKeyboardButton
		for i, h := range withFile {
			if i >= 6 {
				break
			}
			label := h.Caption
			if label == "" {
				label = fmt.Sprintf("Message #%d", h.MessageID)
			}
			if len(label) > 40 {
				label = label[:40] + "…"
			}
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("📄 "+label, fmt.Sprintf("pickreq:%s:%d", reqID, h.MessageID)),
			))
		}
		kb := tgbotapi.NewInlineKeyboardMarkup(rows...)
		msgCfg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔎 \"%s\" ke liye %d matches mile — sahi wali chuno:", req.Name, len(withFile)))
		msgCfg.ReplyMarkup = kb
		a.pool.primary().Send(msgCfg)
	}
}

// importPickedCandidate handles the admin picking one specific message from
// handleFindRequest's multi-match list.
func (a *App) importPickedCandidate(ctx context.Context, chatID int64, msgID int, reqID string, srcMsgID int) {
	srcChannelID, ok := a.cache.getImportSource(ctx)
	if !ok {
		a.pool.send(chatID, "❌ Source channel set nahi hai — /setsource se set karo.")
		return
	}
	rec, err := a.importSingleMessage(ctx, srcChannelID, srcMsgID, a.cfg.AdminID)
	if err != nil {
		a.pool.send(chatID, "❌ Upload fail ho gaya: "+err.Error())
		return
	}
	a.db.markContentRequestFulfilled(ctx, reqID)
	base := a.cfg.baseURL()
	a.pool.sendMD(chatID, fmt.Sprintf(
		"✅ *Upload ho gaya\\!*\n\n📄 `%s`\n🆔 `%s`\n📺 [Watch](%s/watch/%s)",
		mdEscape(rec.FileName), rec.ID, base, rec.ID,
	))
}

func (a *App) onCallback(ctx context.Context, cb *tgbotapi.CallbackQuery) {
	if strings.HasPrefix(cb.Data, "va:") {
		a.onVisitorActionCallback(ctx, cb)
		return
	}
	if strings.HasPrefix(cb.Data, "findreq:") {
		if cb.From.ID != a.cfg.AdminID {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Admin only."))
			return
		}
		reqID := strings.TrimPrefix(cb.Data, "findreq:")
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "🔎 Dhoondh raha hoon..."))
		a.handleFindRequest(ctx, cb.Message.Chat.ID, cb.Message.MessageID, reqID)
		return
	}
	if strings.HasPrefix(cb.Data, "pickreq:") {
		if cb.From.ID != a.cfg.AdminID {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Admin only."))
			return
		}
		// pickreq:<reqID>:<msgID> — admin picked one candidate from a
		// multi-match list.
		parts := strings.SplitN(strings.TrimPrefix(cb.Data, "pickreq:"), ":", 2)
		if len(parts) != 2 {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Bad data."))
			return
		}
		reqID := parts[0]
		msgID, mErr := strconv.Atoi(parts[1])
		if mErr != nil {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Bad data."))
			return
		}
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "⬆️ Upload ho raha hai..."))
		a.importPickedCandidate(ctx, cb.Message.Chat.ID, cb.Message.MessageID, reqID, msgID)
		return
	}

	if strings.HasPrefix(cb.Data, "premapprove:") {
		if cb.From.ID != a.cfg.AdminID {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Admin only."))
			return
		}
		reqID := strings.TrimPrefix(cb.Data, "premapprove:")
		pr, err := a.db.getPremiumRequest(ctx, reqID)
		if err != nil {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Request not found."))
			return
		}
		if pr.Status == "approved" {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "ℹ️ Already approved."))
			return
		}
		code := strings.ToUpper(uuid.New().String()[:8])
		if err := a.db.createPremiumCode(ctx, code, pr.Days, a.cfg.AdminID); err != nil {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Code generation failed."))
			return
		}
		if _, ok, err := a.db.approvePremiumRequest(ctx, reqID, code); err != nil || !ok {
			a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "❌ Approve failed (already handled?)."))
			return
		}
		// Auto-grant premium directly to the requesting device, so the
		// website's polling picks it up with zero action needed from the
		// visitor — the code itself is kept too, purely as a manual
		// fallback the admin can hand out via the contact-message feature
		// if auto-detection is ever missed (browser closed, cookies
		// cleared, different device, etc.).
		expiresAt, gErr := a.db.grantPremiumDirect(ctx, pr.DeviceID, pr.Days)
		if gErr != nil {
			a.logger.Warn("premapprove: grantPremiumDirect failed", zap.Error(gErr), zap.String("device", pr.DeviceID))
		}
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "✅ Approved & activated!"))
		edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID, fmt.Sprintf(
			"✅ Premium Approved\nPlan: %s (%s)\nDevice: %s\nCode: %s\nExpires: %s\n\nDevice pe turant activate ho gaya. Agar visitor ka auto-detect miss ho jaaye, yeh code unhe /contact reply se bhej sakte ho — woh /redeem page pe daal ke bhi activate kar sakte hain.",
			pr.PlanLabel, pr.Price, pr.DeviceID, code, expiresAt.Format("02 Jan 2006, 15:04 MST"),
		))
		a.pool.primary().Send(edit)
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
		// The prompt video, referral-welcome video and ad banner are stored as
		// files too, so deleting everything leaves these settings pointing at
		// dead files (black screen / nothing plays). Clear them as well.
		a.cache.clearPromptVideo(ctx)
		a.cache.clearReferralWelcomeSlug(ctx)
		a.cache.clearAdvertise(ctx)
		a.pool.primary().Request(tgbotapi.NewCallback(cb.ID, "✅ Deleted!"))
		edit := tgbotapi.NewEditMessageText(cb.Message.Chat.ID, cb.Message.MessageID,
			fmt.Sprintf("🗑️ %d files permanently deleted. All links are now dead. Password video, referral video aur ad banner bhi reset ho gaye — dobara /video, /video2, /advertise se set karo.", n))
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
		a.handleHome(w, r)

	case path == "/about":
		a.serveLanding(w)

	case path == "/health" || path == "/ping":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","mtproto":%t}`, a.mtPool.isAnyReady())

	case strings.HasPrefix(path, "/stream/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/stream/"), "/")
		a.handleStream(w, r, slug, false)

	case strings.HasPrefix(path, "/promo/"):
		slug := strings.TrimSuffix(strings.TrimPrefix(path, "/promo/"), "/")
		a.handlePromoStream(w, r, slug)

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

	// Never let the browser cache actual video bytes under this URL. Without
	// this, browsers commonly cache large video/range responses fairly
	// aggressively, so a deleted/expired file could keep "playing" from the
	// browser's own cache even though the server has correctly removed it —
	// looking exactly like a broken delete when it's really a stale local copy.
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, private, max-age=0")
	w.Header().Set("Pragma", "no-cache")

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

	deviceID := getOrSetDeviceID(w, r)
	isPrem := a.db.isPremium(ctx, deviceID)

	if passwordHash != nil && *passwordHash != "" && !isPrem {
		if !hasValidPasswordCookie(r, slug, *passwordHash) {
			http.Error(w, "🔒 This link is password protected. Open the /watch page and enter the password first.", http.StatusUnauthorized)
			return
		}
		setPasswordCookie(w, slug, *passwordHash)
	}

	approval, isNewDevice, aErr := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if aErr != nil {
		a.logger.Warn("approval lookup failed (stream)", zap.Error(aErr))
	}
	if approval != nil && approval.Blocked {
		renderBlocked(w, approval.AccessID, a.cfg.ContactTelegramUsername)
		return
	}
	if !isPrem && (approval == nil || !approval.Approved) {
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

	a.streamTelegramFile(ctx, w, r, messageID, channelID, fileName, mimeType, fileSize, download)
}

// streamTelegramFile does the actual byte-serving over MTProto (Range
// support, headers, streaming the response body) with no auth checks of its
// own — callers decide who's allowed to reach this point. Used by
// handleStream (which gates on password/approval first) and by
// handlePromoStream (intentionally public, no gate — teaser/preview media).
func (a *App) streamTelegramFile(ctx context.Context, w http.ResponseWriter, r *http.Request,
	messageID int, channelID int64, fileName, mimeType string, fileSize int64, download bool) {

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

// handlePromoStream serves media with NO password/approval/expiry gate — for
// content that must play on the password-entry page itself, before a
// visitor has unlocked anything (the /video-set prompt video, currently).
// Only files explicitly published through this path (their slug handed out
// via /promo/<slug>) are reachable here; regular uploads are never exposed
// this way since their slugs are only ever given out as /stream or /watch
// links.
func (a *App) handlePromoStream(w http.ResponseWriter, r *http.Request, slug string) {
	ctx := r.Context()
	if slug == "" {
		http.Error(w, "Missing file ID", http.StatusBadRequest)
		return
	}
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, private, max-age=0")
	w.Header().Set("Pragma", "no-cache")

	var messageID int
	var channelID int64
	var fileName, mimeType string
	var fileSize int64

	if cached := a.cache.getFile(ctx, slug); cached != nil {
		messageID, channelID = cached.MessageID, cached.ChannelID
		fileName, fileSize, mimeType = cached.FileName, cached.FileSize, cached.MimeType
	} else {
		rec, err := a.db.getFileByID(ctx, slug)
		if err != nil {
			http.Error(w, "File not found.", http.StatusNotFound)
			return
		}
		messageID, channelID = rec.MessageID, rec.ChannelID
		fileName, fileSize, mimeType = rec.FileName, rec.FileSize, rec.MimeType
		a.cache.setFile(ctx, slug, &cachedFile{
			MessageID: messageID, ChannelID: channelID,
			FileName: fileName, FileSize: fileSize, MimeType: mimeType,
		})
	}

	a.streamTelegramFile(ctx, w, r, messageID, channelID, fileName, mimeType, fileSize, false)
}

// ============================================================
// TMDB METADATA
// ============================================================
// Admin links a file to a TMDB page with /tmdb <file_id> <link>. We fetch the
// details ONCE from the TMDB API and store a snapshot on the file record (all
// qualities of a multi-quality group get it), so the watch page and the cards
// never have to call TMDB on every view. Needs TMDB_API_KEY in the env (a v3
// API key, or a v4 "API Read Access Token" — both work).

var tmdbAPIBase = "https://api.themoviedb.org/3"

const (
	tmdbPosterBase   = "https://image.tmdb.org/t/p/w500"
	tmdbBackdropBase = "https://image.tmdb.org/t/p/w1280"
	tmdbProfileBase  = "https://image.tmdb.org/t/p/w185"
)

var tmdbLinkRe = regexp.MustCompile(`(?i)(?:themoviedb\.org/|^|\s)(movie|tv)/(\d+)`)

var tmdbHTTP = &http.Client{Timeout: 10 * time.Second}

type TMDBCast struct {
	Name      string `bson:"name" json:"name"`
	Character string `bson:"character,omitempty" json:"character,omitempty"`
	Photo     string `bson:"photo,omitempty" json:"photo,omitempty"`
}

type TMDBInfo struct {
	SourceURL     string     `bson:"source_url" json:"source_url"`
	MediaType     string     `bson:"media_type" json:"media_type"`
	TMDBID        int        `bson:"tmdb_id" json:"tmdb_id"`
	Title         string     `bson:"title" json:"title"`
	OriginalTitle string     `bson:"original_title,omitempty" json:"original_title,omitempty"`
	Tagline       string     `bson:"tagline,omitempty" json:"tagline,omitempty"`
	Overview      string     `bson:"overview,omitempty" json:"overview,omitempty"`
	Rating        float64    `bson:"rating" json:"rating"`
	Votes         int        `bson:"votes" json:"votes"`
	Year          string     `bson:"year,omitempty" json:"year,omitempty"`
	ReleaseDate   string     `bson:"release_date,omitempty" json:"release_date,omitempty"`
	Runtime       int        `bson:"runtime,omitempty" json:"runtime,omitempty"`
	Seasons       int        `bson:"seasons,omitempty" json:"seasons,omitempty"`
	Episodes      int        `bson:"episodes,omitempty" json:"episodes,omitempty"`
	Status        string     `bson:"status,omitempty" json:"status,omitempty"`
	Language      string     `bson:"language,omitempty" json:"language,omitempty"`
	Genres        []string   `bson:"genres,omitempty" json:"genres,omitempty"`
	Director      string     `bson:"director,omitempty" json:"director,omitempty"`
	PosterURL     string     `bson:"poster_url,omitempty" json:"poster_url,omitempty"`
	BackdropURL   string     `bson:"backdrop_url,omitempty" json:"backdrop_url,omitempty"`
	Cast          []TMDBCast `bson:"cast,omitempty" json:"cast,omitempty"`
	TrailerKey    string     `bson:"trailer_key,omitempty" json:"trailer_key,omitempty"`
}

// parseTMDBLink pulls (movie|tv, id) out of a themoviedb.org link such as
// https://www.themoviedb.org/movie/603-the-matrix. A bare "movie/603" or
// "tv/1396" works too.
func parseTMDBLink(s string) (string, int, bool) {
	m := tmdbLinkRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return "", 0, false
	}
	id, err := strconv.Atoi(m[2])
	if err != nil || id <= 0 {
		return "", 0, false
	}
	return strings.ToLower(m[1]), id, true
}

func tmdbFetch(ctx context.Context, apiKey, mediaType string, id int) (*TMDBInfo, error) {
	if apiKey == "" {
		return nil, errors.New("TMDB_API_KEY set nahi hai")
	}
	q := url.Values{}
	q.Set("language", "en-US")
	q.Set("append_to_response", "credits,videos")
	bearer := strings.HasPrefix(apiKey, "eyJ")
	if !bearer {
		q.Set("api_key", apiKey)
	}
	endpoint := fmt.Sprintf("%s/%s/%d?%s", tmdbAPIBase, mediaType, id, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if bearer {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := tmdbHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("TMDB tak pahunch nahi paya: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return nil, errors.New("TMDB API key galat hai")
	case http.StatusNotFound:
		return nil, errors.New("TMDB pe yeh title nahi mila — link check karo")
	default:
		return nil, fmt.Errorf("TMDB ne error diya (HTTP %d)", resp.StatusCode)
	}

	var raw struct {
		Title            string  `json:"title"`
		Name             string  `json:"name"`
		OriginalTitle    string  `json:"original_title"`
		OriginalName     string  `json:"original_name"`
		Tagline          string  `json:"tagline"`
		Overview         string  `json:"overview"`
		VoteAverage      float64 `json:"vote_average"`
		VoteCount        int     `json:"vote_count"`
		ReleaseDate      string  `json:"release_date"`
		FirstAirDate     string  `json:"first_air_date"`
		Runtime          int     `json:"runtime"`
		EpisodeRunTime   []int   `json:"episode_run_time"`
		NumberOfSeasons  int     `json:"number_of_seasons"`
		NumberOfEpisodes int     `json:"number_of_episodes"`
		Status           string  `json:"status"`
		OriginalLanguage string  `json:"original_language"`
		PosterPath       string  `json:"poster_path"`
		BackdropPath     string  `json:"backdrop_path"`
		Genres           []struct {
			Name string `json:"name"`
		} `json:"genres"`
		CreatedBy []struct {
			Name string `json:"name"`
		} `json:"created_by"`
		Credits struct {
			Cast []struct {
				Name        string `json:"name"`
				Character   string `json:"character"`
				ProfilePath string `json:"profile_path"`
			} `json:"cast"`
			Crew []struct {
				Name string `json:"name"`
				Job  string `json:"job"`
			} `json:"crew"`
		} `json:"credits"`
		Videos struct {
			Results []struct {
				Key      string `json:"key"`
				Site     string `json:"site"`
				Type     string `json:"type"`
				Official bool   `json:"official"`
			} `json:"results"`
		} `json:"videos"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&raw); err != nil {
		return nil, fmt.Errorf("TMDB ka jawab padh nahi paya: %w", err)
	}

	info := &TMDBInfo{
		SourceURL:     fmt.Sprintf("https://www.themoviedb.org/%s/%d", mediaType, id),
		MediaType:     mediaType,
		TMDBID:        id,
		Title:         raw.Title,
		OriginalTitle: raw.OriginalTitle,
		Tagline:       raw.Tagline,
		Overview:      raw.Overview,
		Rating:        raw.VoteAverage,
		Votes:         raw.VoteCount,
		ReleaseDate:   raw.ReleaseDate,
		Runtime:       raw.Runtime,
		Seasons:       raw.NumberOfSeasons,
		Episodes:      raw.NumberOfEpisodes,
		Status:        raw.Status,
		Language:      strings.ToUpper(raw.OriginalLanguage),
	}
	if mediaType == "tv" {
		info.Title = raw.Name
		info.OriginalTitle = raw.OriginalName
		info.ReleaseDate = raw.FirstAirDate
		if len(raw.EpisodeRunTime) > 0 {
			info.Runtime = raw.EpisodeRunTime[0]
		}
	}
	if info.Title == "" {
		return nil, errors.New("TMDB ne is title ka naam nahi diya")
	}
	if len(info.ReleaseDate) >= 4 {
		info.Year = info.ReleaseDate[:4]
	}
	for _, g := range raw.Genres {
		info.Genres = append(info.Genres, g.Name)
	}
	if raw.PosterPath != "" {
		info.PosterURL = tmdbPosterBase + raw.PosterPath
	}
	if raw.BackdropPath != "" {
		info.BackdropURL = tmdbBackdropBase + raw.BackdropPath
	}

	var directors []string
	if mediaType == "tv" {
		for _, c := range raw.CreatedBy {
			directors = append(directors, c.Name)
		}
	} else {
		for _, c := range raw.Credits.Crew {
			if c.Job == "Director" {
				directors = append(directors, c.Name)
			}
		}
	}
	if len(directors) > 3 {
		directors = directors[:3]
	}
	info.Director = strings.Join(directors, ", ")

	for i, c := range raw.Credits.Cast {
		if i >= 12 {
			break
		}
		cm := TMDBCast{Name: c.Name, Character: c.Character}
		if c.ProfilePath != "" {
			cm.Photo = tmdbProfileBase + c.ProfilePath
		}
		info.Cast = append(info.Cast, cm)
	}

	// Trailer: official YouTube trailer first, then any trailer, then a teaser.
	pick := func(typ string, officialOnly bool) string {
		for _, v := range raw.Videos.Results {
			if v.Site == "YouTube" && v.Type == typ && v.Key != "" && (!officialOnly || v.Official) {
				return v.Key
			}
		}
		return ""
	}
	for _, try := range []struct {
		typ  string
		only bool
	}{{"Trailer", true}, {"Trailer", false}, {"Teaser", false}} {
		if k := pick(try.typ, try.only); k != "" {
			info.TrailerKey = k
			break
		}
	}
	return info, nil
}

// ---- Automatic TMDB matching (on upload) ----
// Turns a messy upload name like "Avengers.Endgame.2019.480p.HDTC.Hindi.x264.mkv"
// into ("Avengers Endgame", 2019), searches TMDB, and links the best match.
// The admin gets a message with the match so a wrong one can be fixed with
// /tmdb <file_id> <link>.

var (
	tmdbPrefixRe = regexp.MustCompile(`(?i)^(?:\s*\[[^\]]*\]|\s*www\.[^\s]+\s*[-–]?)+`)
	tmdbYearRe   = regexp.MustCompile(`\b(?:19|20)\d{2}\b`)
	tmdbNoiseRe  = regexp.MustCompile(`(?i)\b(?:\d{3,4}p|4k|uhd|hdtc|hdcam|hdts|hdrip|camrip|cam|webrip|web-?dl|web|blu-?ray|brrip|bdrip|dvdrip|hdtv|hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|dual|multi|x26[45]|h\.?26[45]|hevc|aac|ddp?\d?|esubs?|line|s\d{1,2}e\d{1,2}|season|complete|nf|amzn|zee5|hotstar)\b`)
	tmdbExtRe    = regexp.MustCompile(`(?i)\.(mkv|mp4|avi|mov|webm|m4v|ts|flv)$`)
	tmdbNonWord  = regexp.MustCompile(`[^\p{L}\p{N}]+`)
	tmdbJunkRe   = regexp.MustCompile(`(?i)\{[^}]*\}|@\w+|\bmovies?\s+uploaded\b|\buploaded\s+by\b`)
)

// cleanTitleFromFilename returns the likely movie/series title and release
// year (0 if none) found in a file name.
func cleanTitleFromFilename(name string) (string, int) {
	s := tmdbExtRe.ReplaceAllString(strings.TrimSpace(name), "")
	s = tmdbJunkRe.ReplaceAllString(s, " ")
	s = tmdbPrefixRe.ReplaceAllString(strings.TrimSpace(s), "")
	s = strings.NewReplacer(".", " ", "_", " ", "[", " ", "]", " ", "(", " ", ")", " ").Replace(s)
	s = strings.Join(strings.Fields(s), " ")

	cut := len(s)
	if loc := tmdbNoiseRe.FindStringIndex(s); loc != nil && loc[0] > 0 {
		cut = loc[0]
	}
	year := 0
	prefix := s[:cut]
	if ys := tmdbYearRe.FindAllStringIndex(prefix, -1); len(ys) > 0 {
		last := ys[len(ys)-1]
		if last[0] > 0 { // a title that IS a year (e.g. "2012") keeps it
			year, _ = strconv.Atoi(prefix[last[0]:last[1]])
			prefix = prefix[:last[0]]
		}
	}
	title := strings.Trim(strings.TrimSpace(prefix), "-–—:| ")
	return title, year
}

// tmdbAutoFind searches TMDB for a file name and returns the best match.
func tmdbAutoFind(ctx context.Context, apiKey, fileName string) (string, int, bool) {
	title, year := cleanTitleFromFilename(fileName)
	if len([]rune(title)) < 2 {
		return "", 0, false
	}
	q := url.Values{}
	q.Set("query", title)
	q.Set("include_adult", "false")
	q.Set("language", "en-US")
	bearer := strings.HasPrefix(apiKey, "eyJ")
	if !bearer {
		q.Set("api_key", apiKey)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tmdbAPIBase+"/search/multi?"+q.Encode(), nil)
	if err != nil {
		return "", 0, false
	}
	req.Header.Set("Accept", "application/json")
	if bearer {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := tmdbHTTP.Do(req)
	if err != nil {
		return "", 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, false
	}
	var out struct {
		Results []struct {
			ID           int    `json:"id"`
			MediaType    string `json:"media_type"`
			Title        string `json:"title"`
			Name         string `json:"name"`
			ReleaseDate  string `json:"release_date"`
			FirstAirDate string `json:"first_air_date"`
		} `json:"results"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&out) != nil {
		return "", 0, false
	}
	norm := func(s string) string { return strings.ToLower(tmdbNonWord.ReplaceAllString(s, "")) }
	want := norm(title)

	type cand struct {
		mt   string
		id   int
		yr   int
		same bool
	}
	var cands []cand
	for _, r := range out.Results {
		if r.MediaType != "movie" && r.MediaType != "tv" {
			continue
		}
		nm, date := r.Title, r.ReleaseDate
		if r.MediaType == "tv" {
			nm, date = r.Name, r.FirstAirDate
		}
		yr := 0
		if len(date) >= 4 {
			yr, _ = strconv.Atoi(date[:4])
		}
		cands = append(cands, cand{r.MediaType, r.ID, yr, norm(nm) == want})
	}
	if len(cands) == 0 {
		return "", 0, false
	}
	abs := func(x int) int {
		if x < 0 {
			return -x
		}
		return x
	}
	if year > 0 {
		for _, c := range cands { // exact year first, then +-1
			if c.yr == year {
				return c.mt, c.id, true
			}
		}
		for _, c := range cands {
			if c.yr != 0 && abs(c.yr-year) <= 1 {
				return c.mt, c.id, true
			}
		}
		for _, c := range cands { // same title, different year: still the right title
			if c.same {
				return c.mt, c.id, true
			}
		}
		return "", 0, false
	}
	return cands[0].mt, cands[0].id, true
}

var tmdbGroupLocks sync.Map // key -> *sync.Mutex, so a multi-quality group is matched once

// autoTMDBOne finds + stores TMDB info for one file. Returns the info if it
// was newly linked (nil if skipped, copied from a sibling quality, or no match)
// plus a short status for logging.
func (a *App) autoTMDBOne(ctx context.Context, rec *FileRecord) (*TMDBInfo, string) {
	if a.cfg.TMDBAPIKey == "" || rec == nil || rec.Hidden || rec.TMDB != nil {
		return nil, "skipped"
	}
	key := "f:" + rec.ID
	if rec.GroupID != nil && *rec.GroupID != "" {
		key = "g:" + *rec.GroupID
	}
	mu, _ := tmdbGroupLocks.LoadOrStore(key, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	if rec.GroupID != nil && *rec.GroupID != "" {
		if members, err := a.db.listGroupFiles(ctx, *rec.GroupID); err == nil {
			for _, m := range members {
				if m.TMDB != nil {
					a.db.setFileTMDB(ctx, rec.ID, m.TMDB)
					return nil, "copied"
				}
			}
		}
	}
	mt, id, ok := tmdbAutoFind(ctx, a.cfg.TMDBAPIKey, rec.FileName)
	if !ok {
		return nil, "nomatch"
	}
	info, err := tmdbFetch(ctx, a.cfg.TMDBAPIKey, mt, id)
	if err != nil {
		return nil, "error"
	}
	if _, err := a.db.setFileTMDB(ctx, rec.ID, info); err != nil {
		return nil, "error"
	}
	return info, "linked"
}

// tmdbAutoSem limits how many background TMDB matches run at once, so a big
// /import (hundreds of files) can't flood TMDB or pile up goroutines.
var tmdbAutoSem = make(chan struct{}, 2)

// autoTMDB runs in the background right after an upload is saved, so the
// upload itself is never slowed down or failed by TMDB. The admin gets a
// message with the match.
func (a *App) autoTMDB(rec *FileRecord) { a.autoTMDBMode(rec, false) }

// autoTMDBMode is autoTMDB with an optional quiet mode (used by /import so a
// bulk import doesn't send one Telegram message per file).
func (a *App) autoTMDBMode(rec *FileRecord, quiet bool) {
	if a.cfg.TMDBAPIKey == "" || rec == nil || rec.Hidden {
		return
	}
	go func() {
		tmdbAutoSem <- struct{}{}
		defer func() { <-tmdbAutoSem }()
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		info, status := a.autoTMDBOne(ctx, rec)
		if quiet {
			time.Sleep(250 * time.Millisecond)
			return
		}
		switch status {
		case "linked":
			a.pool.send(a.cfg.AdminID, fmt.Sprintf("🎬 Auto TMDB: %s\n→ %s (%s) ⭐ %.1f\n\nGalat match ho to: /tmdb %s <sahi tmdb link>",
				rec.FileName, info.Title, info.Year, info.Rating, rec.ID))
		case "nomatch":
			a.pool.send(a.cfg.AdminID, fmt.Sprintf("⚠️ TMDB pe match nahi mila: %s\n\nKhud lagao: /tmdb %s <tmdb link>", rec.FileName, rec.ID))
		}
	}()
}

// listFilesWithoutTMDB returns visible files that have no TMDB info yet.
func (db *DB) listFilesWithoutTMDB(ctx context.Context, limit int) ([]*FileRecord, error) {
	cursor, err := db.files.Find(ctx, bson.M{"hidden": bson.M{"$ne": true}, "tmdb": bson.M{"$exists": false}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	if err := cursor.All(ctx, &out); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// runAutoTMDBBackfill links TMDB info for existing files that don't have it
// (newest first, up to `limit` files per run), then reports to the admin.
func (a *App) runAutoTMDBBackfill(limit int) {
	ctx := context.Background()
	files, err := a.db.listFilesWithoutTMDB(ctx, limit)
	if err != nil {
		a.pool.send(a.cfg.AdminID, "❌ Files list nahi mil payi, dobara try karo.")
		return
	}
	linked, missed := 0, 0
	var missedNames []string
	for _, f := range files {
		fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, status := a.autoTMDBOne(fctx, f)
		cancel()
		switch status {
		case "linked", "copied":
			linked++
		case "nomatch", "error":
			missed++
			if len(missedNames) < 15 {
				missedNames = append(missedNames, fmt.Sprintf("• %s\n  /tmdb %s <link>", f.FileName, f.ID))
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	msg := fmt.Sprintf("✅ Auto TMDB backfill poora: %d files link hui, %d ka match nahi mila.", linked, missed)
	if len(missedNames) > 0 {
		msg += "\n\nIn ko khud lagao:\n" + strings.Join(missedNames, "\n")
	}
	if len(msg) > 3900 {
		msg = msg[:3900] + "…"
	}
	a.pool.send(a.cfg.AdminID, msg)
}

func tmdbPoster(f *FileRecord) string {
	if f == nil || f.TMDB == nil {
		return ""
	}
	return f.TMDB.PosterURL
}

func tmdbRating(f *FileRecord) float64 {
	if f == nil || f.TMDB == nil {
		return 0
	}
	return f.TMDB.Rating
}

// setFileTMDB stores (or, with info == nil, removes) the TMDB snapshot on a
// file. If the file belongs to a multi-quality group, every quality of the
// group gets it, so whichever variant is shown as the card has the poster.
func (db *DB) setFileTMDB(ctx context.Context, fileID string, info *TMDBInfo) (bool, error) {
	var rec FileRecord
	if err := db.files.FindOne(ctx, bson.M{"_id": fileID}).Decode(&rec); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	filter := bson.M{"_id": fileID}
	if rec.GroupID != nil && *rec.GroupID != "" {
		filter = bson.M{"group_id": *rec.GroupID}
	}
	var update bson.M
	if info == nil {
		update = bson.M{"$unset": bson.M{"tmdb": ""}}
	} else {
		update = bson.M{"$set": bson.M{"tmdb": info}}
	}
	_, err := db.files.UpdateMany(ctx, filter, update)
	return true, err
}

// handleMeta serves the TMDB snapshot for one file as JSON ("{}" if none) —
// the watch page calls it when a viewer switches to another video in place.
func (a *App) handleMeta(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/meta/"), "/")
	if id == "" {
		fmt.Fprint(w, `{}`)
		return
	}
	rec, err := a.db.getFileByID(ctx, id)
	if err != nil || rec.TMDB == nil {
		fmt.Fprint(w, `{}`)
		return
	}
	b, _ := json.Marshal(rec.TMDB)
	w.Write(b)
}

// ---- Sponsor / ad link (a clickable link for advertisers) ----

type adLinkSettings struct {
	URL  string `json:"url"`
	Text string `json:"text"`
}

const adLinkKey = "site:adlink"

func (c *Cache) setAdLink(ctx context.Context, l *adLinkSettings) error {
	b, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, adLinkKey, b, 0).Err()
}

func (c *Cache) getAdLink(ctx context.Context) *adLinkSettings {
	b, err := c.client.Get(ctx, adLinkKey).Bytes()
	if err != nil {
		return nil
	}
	var l adLinkSettings
	if json.Unmarshal(b, &l) != nil || l.URL == "" {
		return nil
	}
	return &l
}

func (c *Cache) clearAdLink(ctx context.Context) { c.client.Del(ctx, adLinkKey) }

// ============================================================
// HOME PAGE — one card per title, click to see every file/quality
// ============================================================
// Files that belong to the same title (same TMDB id, or the same cleaned-up
// name like "Pushpa") are grouped into ONE card on the home page. Clicking
// the card lists all of that title's files (every quality / size / episode);
// clicking a file opens the normal /watch/<id> stream page, which is not
// touched by any of this.

var homeEpisodeRe = regexp.MustCompile(`(?i)\bs(\d{1,2})[ ._-]?e(\d{1,3})\b`)

type homeFile struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Quality string `json:"quality"`
	Episode string `json:"episode,omitempty"`
	Size    string `json:"size"`
	Locked  bool   `json:"locked"`

	rank    int
	bytes   int64
	views   int64
	created time.Time
}

type homeTitle struct {
	Key        string
	Title      string
	Year       int
	Poster     string
	Rating     float64
	MetaFileID string // a file id whose /meta/<id> has the full TMDB info ("" = none)
	Category   string // Subject tag, else "Entertainment" (TMDB-linked), else "Other"
	Files      []homeFile

	newest   time.Time
	views    int64
	search   string
	subjects map[string]int
}

type homeCat struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type homeIndex struct {
	built  time.Time
	titles []*homeTitle
	byKey  map[string]*homeTitle
	cats   []homeCat
}

func homeNameKey(s string) string { return strings.ToLower(tmdbNonWord.ReplaceAllString(s, "")) }

func homeShortKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])[:10]
}

// buildHomeTitles groups files (expected newest-first) into titles.
func buildHomeTitles(files []*FileRecord) []*homeTitle {
	// Pass 1: remember which TMDB title each file name belongs to, so files
	// of the same name that have no TMDB info join the same card.
	nameToTMDB := map[string]string{}
	for _, f := range files {
		if f.TMDB == nil || f.TMDB.TMDBID <= 0 {
			continue
		}
		key := fmt.Sprintf("tmdb:%s:%d", f.TMDB.MediaType, f.TMDB.TMDBID)
		t, _ := cleanTitleFromFilename(f.FileName)
		for _, n := range []string{homeNameKey(t), homeNameKey(f.TMDB.Title)} {
			if n != "" {
				if _, ok := nameToTMDB[n]; !ok {
					nameToTMDB[n] = key
				}
			}
		}
	}

	byKey := map[string]*homeTitle{}
	var order []*homeTitle
	get := func(key, title string, year int) *homeTitle {
		g := byKey[key]
		if g == nil {
			g = &homeTitle{Key: homeShortKey(key), Title: title, Year: year}
			byKey[key] = g
			order = append(order, g)
		}
		return g
	}

	for _, f := range files {
		t, y := cleanTitleFromFilename(f.FileName)
		n := homeNameKey(t)
		var g *homeTitle
		switch {
		case f.TMDB != nil && f.TMDB.TMDBID > 0:
			key := fmt.Sprintf("tmdb:%s:%d", f.TMDB.MediaType, f.TMDB.TMDBID)
			g = get(key, t, y)
			// real TMDB data always wins over the guessed name
			g.Title = f.TMDB.Title
			if yr, err := strconv.Atoi(f.TMDB.Year); err == nil {
				g.Year = yr
			}
			if g.Poster == "" {
				g.Poster = f.TMDB.PosterURL
			}
			if g.Rating == 0 {
				g.Rating = f.TMDB.Rating
			}
			if g.MetaFileID == "" {
				g.MetaFileID = f.ID
			}
		case n != "" && nameToTMDB[n] != "":
			g = get(nameToTMDB[n], t, y)
		case n != "":
			key := "n:" + n
			if ex := byKey[key]; ex != nil && ex.Year != 0 && y != 0 && ex.Year != y {
				key = fmt.Sprintf("n:%s:%d", n, y) // same name, different year = different title
			}
			g = get(key, t, y)
		default:
			g = get("f:"+f.ID, tmdbExtRe.ReplaceAllString(f.FileName, ""), 0)
		}

		label, rank := f.QualityLabel, f.QualityRank
		if label == "" {
			label, rank = detectQuality(f.FileName)
		}
		if label == "" {
			label = "File"
		}
		ep := ""
		if m := homeEpisodeRe.FindStringSubmatch(f.FileName); m != nil {
			s, _ := strconv.Atoi(m[1])
			e, _ := strconv.Atoi(m[2])
			ep = fmt.Sprintf("S%02dE%02d", s, e)
		}
		g.Files = append(g.Files, homeFile{
			ID: f.ID, Name: f.FileName, Quality: label, Episode: ep,
			Size:   formatSize(f.FileSize),
			Locked: f.PasswordHash != nil && *f.PasswordHash != "",
			rank:   rank, bytes: f.FileSize, views: f.ViewCount, created: f.CreatedAt,
		})
		g.views += f.ViewCount
		if sj := strings.TrimSpace(f.Subject); sj != "" {
			if g.subjects == nil {
				g.subjects = map[string]int{}
			}
			g.subjects[sj]++
		}
		if f.CreatedAt.After(g.newest) {
			g.newest = f.CreatedAt
		}
	}

	for _, g := range order {
		if strings.TrimSpace(g.Title) == "" {
			g.Title = g.Files[0].Name
		}
		sort.SliceStable(g.Files, func(i, j int) bool {
			a, b := g.Files[i], g.Files[j]
			if a.Episode != b.Episode {
				return a.Episode < b.Episode
			}
			if a.rank != b.rank {
				return a.rank > b.rank
			}
			return a.bytes > b.bytes
		})
		var sb strings.Builder
		sb.WriteString(strings.ToLower(g.Title))
		for _, f := range g.Files {
			sb.WriteByte(' ')
			sb.WriteString(strings.ToLower(f.Name))
		}
		g.search = sb.String()

		// category: the most common Subject tag; otherwise movies/series that
		// have TMDB info are "Entertainment"; everything else is "Other".
		best, bestN := "", 0
		for name, n := range g.subjects {
			if n > bestN || (n == bestN && name < best) {
				best, bestN = name, n
			}
		}
		switch {
		case best != "":
			g.Category = best
		case g.MetaFileID != "":
			g.Category = "Entertainment"
		default:
			g.Category = "Other"
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return order[i].newest.After(order[j].newest) })
	return order
}

// listAllVisibleFiles loads every browsable file, newest first, without the
// heavy TMDB fields (cast/overview) the grouped list doesn't need.
func (db *DB) listAllVisibleFiles(ctx context.Context) ([]*FileRecord, error) {
	// No DB-side sort: Mongo (free tier) aborts sorts over 32MB without an index.
	// Sorting in Go instead is cheap and always works.
	opts := options.Find().
		SetLimit(30000).
		SetProjection(bson.M{"password_plain": 0, "description": 0, "tmdb.cast": 0, "tmdb.overview": 0, "tmdb.tagline": 0})
	cursor, err := db.files.Find(ctx, bson.M{"hidden": bson.M{"$ne": true}}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []*FileRecord
	if err := cursor.All(ctx, &out); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

var (
	homeIdxMu sync.Mutex
	homeIdx   *homeIndex
)

const homeIndexTTL = 45 * time.Second

// getHomeIndex returns the grouped title index, rebuilt at most every 45s.
func (a *App) getHomeIndex() *homeIndex {
	homeIdxMu.Lock()
	defer homeIdxMu.Unlock()
	if homeIdx != nil && time.Since(homeIdx.built) < homeIndexTTL {
		return homeIdx
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	files, err := a.db.listAllVisibleFiles(ctx)
	if err != nil {
		a.logger.Error("home index: list files failed", zap.Error(err))
		if homeIdx != nil {
			return homeIdx
		}
		return &homeIndex{built: time.Now(), byKey: map[string]*homeTitle{}}
	}
	titles := buildHomeTitles(files)
	idx := &homeIndex{built: time.Now(), titles: titles, byKey: make(map[string]*homeTitle, len(titles))}
	counts := map[string]int{}
	for _, t := range titles {
		idx.byKey[t.Key] = t
		counts[t.Category]++
	}
	for name, n := range counts {
		idx.cats = append(idx.cats, homeCat{name, n})
	}
	// subjects first (biggest first), then Entertainment, then Other
	rankOf := func(n string) int {
		switch n {
		case "Entertainment":
			return 1
		case "Other":
			return 2
		}
		return 0
	}
	sort.Slice(idx.cats, func(i, j int) bool {
		a, b := idx.cats[i], idx.cats[j]
		if rankOf(a.Name) != rankOf(b.Name) {
			return rankOf(a.Name) < rankOf(b.Name)
		}
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.Name < b.Name
	})
	homeIdx = idx
	return idx
}

// GET /api/titles?q=&sort=new|popular&page=0
func (a *App) handleAPITitles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 0 {
		page = 0
	}
	if r.URL.Query().Get("debug") == "1" {
		dctx, dcancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer dcancel()
		all, e1 := a.db.countFiles(dctx)
		hid, e2 := a.db.files.CountDocuments(dctx, bson.M{"hidden": true})
		vis, e3 := a.db.listAllVisibleFiles(dctx)
		es := func(e error) string {
			if e == nil {
				return ""
			}
			return e.Error()
		}
		var sample []string
		for i, f := range vis {
			if i >= 5 {
				break
			}
			sample = append(sample, f.FileName)
		}
		b, _ := json.Marshal(map[string]any{"db_files_total": all, "db_hidden": hid, "visible_listed": len(vis), "sample": sample,
			"err_count": es(e1), "err_hidden": es(e2), "err_list": es(e3)})
		w.Write(b)
		return
	}
	idx := a.getHomeIndex()
	list := idx.titles
	if cat := strings.TrimSpace(r.URL.Query().Get("cat")); cat != "" {
		var f []*homeTitle
		for _, t := range list {
			if strings.EqualFold(t.Category, cat) {
				f = append(f, t)
			}
		}
		list = f
	}
	if q != "" {
		var f []*homeTitle
		for _, t := range list {
			if strings.Contains(t.search, q) {
				f = append(f, t)
			}
		}
		list = f
	}
	if r.URL.Query().Get("sort") == "popular" {
		c := append([]*homeTitle(nil), list...)
		sort.SliceStable(c, func(i, j int) bool { return c[i].views > c[j].views })
		list = c
	}

	const per = 24
	total := len(list)
	start := page * per
	if start > total {
		start = total
	}
	end := start + per
	if end > total {
		end = total
	}
	type item struct {
		Key       string   `json:"key"`
		Title     string   `json:"title"`
		Year      int      `json:"year,omitempty"`
		Poster    string   `json:"poster,omitempty"`
		Rating    float64  `json:"rating,omitempty"`
		Count     int      `json:"count"`
		Qualities []string `json:"qualities,omitempty"`
		Category  string   `json:"category,omitempty"`
	}
	items := make([]item, 0, end-start)
	for _, t := range list[start:end] {
		seen := map[string]bool{}
		var qs []string
		for _, f := range t.Files { // already highest quality first
			if f.Quality != "File" && !seen[f.Quality] {
				seen[f.Quality] = true
				qs = append(qs, f.Quality)
			}
		}
		if len(qs) > 4 {
			qs = qs[:4]
		}
		items = append(items, item{t.Key, t.Title, t.Year, t.Poster, t.Rating, len(t.Files), qs, t.Category})
	}
	b, _ := json.Marshal(map[string]any{"items": items, "has_more": end < total, "total": total, "cats": idx.cats})
	w.Write(b)
}

// GET /api/title/<key> — every file of one title.
func (a *App) handleAPITitle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	key := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/title/"), "/")
	t := a.getHomeIndex().byKey[key]
	if t == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	b, _ := json.Marshal(map[string]any{
		"key": t.Key, "title": t.Title, "year": t.Year, "poster": t.Poster, "rating": t.Rating,
		"meta_file_id": t.MetaFileID, "category": t.Category, "files": t.Files,
	})
	w.Write(b)
}

type homePageData struct {
	AdEnabled  bool
	AdType     string
	AdURL      string
	AdLinkURL  string
	AdLinkText string
	AdSlotHTML template.HTML
	AdBodyHTML template.HTML
	WhatsAppURL string
}

var (
	homeTmplOnce sync.Once
	homeTmpl     *template.Template
)

// handleHome serves the grouped home page. The page lives inside this file
// (homePageHTML below), so it can never go missing from a deploy.
func (a *App) handleHome(w http.ResponseWriter, r *http.Request) {
	homeTmplOnce.Do(func() {
		t, err := template.New("home").Parse(homePageHTML)
		if err != nil {
			a.logger.Error("home page template is broken, using the simple landing page", zap.Error(err))
			return
		}
		homeTmpl = t
	})
	if homeTmpl == nil {
		a.serveLanding(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	d := homePageData{
		AdSlotHTML: template.HTML(a.cfg.AdBannerHTML),
		AdBodyHTML: template.HTML(a.cfg.AdBodyHTML),
	}
	if ad := a.cache.getAdvertise(ctx); ad != nil {
		d.AdEnabled, d.AdType, d.AdURL = true, ad.Type, ad.URL
	}
	if l := a.cache.getAdLink(ctx); l != nil {
		d.AdLinkURL, d.AdLinkText = l.URL, l.Text
	} else if a.cfg.AdDirectLink != "" {
		d.AdLinkURL, d.AdLinkText = a.cfg.AdDirectLink, a.cfg.AdLinkText
		if d.AdLinkText == "" {
			d.AdLinkText = "Visit Sponsor"
		}
	}
	if a.cfg.WhatsAppNumber != "" {
		txt := a.cfg.WhatsAppText
		if txt == "" {
			txt = "Hello, mujhe coaching ke baare mein jaankari chahiye."
		}
		d.WhatsAppURL = "https://wa.me/" + a.cfg.WhatsAppNumber + "?text=" + strings.ReplaceAll(url.QueryEscape(txt), "+", "%20")
	}
	var buf bytes.Buffer
	if err := homeTmpl.Execute(&buf, d); err != nil {
		a.logger.Error("home template failed", zap.Error(err))
		a.serveLanding(w)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write(buf.Bytes())
}

// ---- "Install as app" (PWA): manifest, icon and a do-nothing service worker ----

const pwaIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#19e3ff"/><stop offset="1" stop-color="#ff3d8b"/></linearGradient></defs><rect width="512" height="512" rx="112" fill="#07080f"/><rect x="24" y="24" width="464" height="464" rx="92" fill="none" stroke="url(#g)" stroke-width="14"/><text x="256" y="350" text-anchor="middle" font-family="Arial Black,Arial,sans-serif" font-weight="900" font-size="300" fill="url(#g)">A</text></svg>`

func (a *App) handleManifest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/manifest+json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	b, _ := json.Marshal(map[string]any{
		"name": "AstraToonix", "short_name": "AstraToonix", "description": "Watch and download — every quality in one place.",
		"start_url": "/", "scope": "/", "display": "standalone",
		"background_color": "#07080f", "theme_color": "#07080f",
		"icons": []map[string]string{
			{"src": "/icon.svg", "sizes": "192x192 512x512", "type": "image/svg+xml", "purpose": "any"},
		},
	})
	w.Write(b)
}

func (a *App) handleIcon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	fmt.Fprint(w, pwaIconSVG)
}

func (a *App) handleServiceWorker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Service-Worker-Allowed", "/")
	// No caching on purpose: it only makes the site installable and never
	// serves stale pages or video.
	fmt.Fprint(w, "self.addEventListener('install',function(){self.skipWaiting();});self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim());});self.addEventListener('fetch',function(){});")
}

func (a *App) serveLanding(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `<html><body style="background:#1b1c26;color:#7aa2f7;font-family:monospace;text-align:center;padding:50px">
<h1>📚 Astratoonix Education Platform</h1>
<p style="color:#8b949e">Science &middot; Math &middot; Physics &middot; Chemistry &middot; English &middot; Coding (Java, C, Python, Go &amp; more)</p>
<p style="color:#484f58;font-size:12px">A self-hosted learning platform for students, built by Raj Dev.</p>
</body></html>`)
}

// homePageHTML is the home page template (same content as home.html).
const homePageHTML = `<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AstraToonix — Stream the Universe</title>
<meta name="description" content="Watch and download — every quality in one place.">
<meta name="theme-color" content="#07080f">
<link rel="manifest" href="/manifest.webmanifest">
<link rel="icon" href="/icon.svg" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@700;900&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #07080f; --bg2: #0e1018; --card: #12141f; --card2: #151827; --line: #1f2433;
    --text: #eef1f8; --muted: #8b93a7;
    --cyan: #19e3ff; --pink: #ff3d8b; --gold: #ffd166; --green: #21d07a;
    --on-cyan: #04131a; --radius: 14px;
  }
  :root[data-theme="light"] {
    --bg: #f3f5fb; --bg2: #ffffff; --card: #ffffff; --card2: #f0f3fa; --line: #dde2ef;
    --text: #10131c; --muted: #5b6479; --cyan: #0a8fb0; --on-cyan: #ffffff; --gold: #b7791f;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
  html { scroll-behavior: smooth; }
  body {
    background: radial-gradient(1200px 500px at 50% -120px, rgba(25,227,255,.10), transparent 70%), var(--bg);
    color: var(--text); font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
    min-height: 100vh; padding-bottom: 90px;
  }
  a { color: inherit; text-decoration: none; }
  button { font: inherit; color: inherit; }
  .wrap { max-width: 1180px; margin: 0 auto; padding: 0 16px; }
  [hidden] { display: none !important; }

  /* ---------- header ---------- */
  header { text-align: center; padding: 26px 16px 8px; }
  .logo {
    font-family: 'Orbitron', sans-serif; font-weight: 900; font-size: clamp(26px, 6vw, 40px); letter-spacing: 2px;
    background: linear-gradient(90deg, var(--cyan), var(--pink)); -webkit-background-clip: text; background-clip: text; color: transparent;
    cursor: pointer;
  }
  .tagline { color: var(--muted); font-size: 12px; letter-spacing: 3px; text-transform: uppercase; margin-top: 4px; }

  /* ---------- notice banner (from /reply) ---------- */
  .notice {
    max-width: 1180px; margin: 12px auto 0; padding: 0 16px;
  }
  .notice-in {
    display: flex; align-items: center; gap: 10px; border-radius: 12px; padding: 11px 14px; font-weight: 600; font-size: 14px; line-height: 1.4;
    background: linear-gradient(90deg, rgba(255,61,139,.18), rgba(25,227,255,.16)); border: 1px solid rgba(255,61,139,.45);
    box-shadow: 0 0 22px rgba(255,61,139,.15);
  }
  .notice-in .ico { font-size: 18px; flex-shrink: 0; }
  .notice-in .txt { flex: 1; min-width: 0; word-break: break-word; }
  .notice-in button { background: transparent; border: 0; color: var(--muted); font-size: 18px; cursor: pointer; padding: 2px 6px; }

  /* ---------- ad zones ---------- */
  .ad-zone { margin: 14px auto 0; max-width: 1180px; padding: 0 16px; }
  .ad-top { position: relative; border-radius: var(--radius); overflow: hidden; border: 1px solid var(--line); background: var(--bg2); }
  .ad-top img, .ad-top video { display: block; width: 100%; max-height: 260px; object-fit: cover; }
  .ad-badge {
    position: absolute; top: 8px; left: 8px; z-index: 2; font-size: 10px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase;
    background: rgba(7,8,15,.8); color: #ffd166; border: 1px solid rgba(255,209,102,.5); padding: 2px 8px; border-radius: 20px;
  }
  .ad-slot { display: flex; justify-content: center; overflow: auto; }
  .sponsor-strip {
    display: flex; align-items: center; gap: 10px; padding: 12px 14px; border-radius: 12px; font-weight: 700; font-size: 14px;
    background: var(--card); border: 1px dashed rgba(255,209,102,.7); box-shadow: 0 0 18px rgba(255,209,102,.10);
    transition: transform .15s ease, box-shadow .15s ease;
  }
  .sponsor-strip:hover { transform: translateY(-2px); box-shadow: 0 0 26px rgba(255,209,102,.25); }
  .sponsor-tag {
    background: linear-gradient(90deg, var(--pink), #ffd166); color: #14101d; font-size: 10.5px; font-weight: 800;
    letter-spacing: .06em; text-transform: uppercase; padding: 3px 9px; border-radius: 20px; flex-shrink: 0;
  }
  .sponsor-text { flex: 1; min-width: 0; word-break: break-word; }

  /* ---------- continue watching ---------- */
  .cw { margin-top: 18px; }
  .sec-h { display: flex; align-items: baseline; justify-content: space-between; margin: 0 2px 10px; }
  .sec-h h2 { font-size: 16px; }
  .sec-h button { background: none; border: 0; color: var(--muted); font-size: 12px; cursor: pointer; }
  .cw-row { display: flex; gap: 12px; overflow-x: auto; padding-bottom: 8px; scroll-snap-type: x proximity; scrollbar-width: thin; }
  .cw-card {
    flex: 0 0 230px; scroll-snap-align: start; background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 12px 12px 10px;
    display: block; transition: border-color .15s, transform .15s;
  }
  .cw-card:hover { border-color: var(--cyan); transform: translateY(-3px); }
  .cw-name { font-size: 13px; font-weight: 700; line-height: 1.35; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; word-break: break-word; min-height: 35px; }
  .cw-meta { display: flex; align-items: center; justify-content: space-between; margin-top: 8px; font-size: 11.5px; color: var(--muted); }
  .cw-play { background: var(--pink); color: #fff; border-radius: 20px; padding: 3px 10px; font-weight: 700; font-size: 11.5px; }
  .bar { height: 4px; border-radius: 4px; background: rgba(127,134,160,.28); overflow: hidden; margin-top: 10px; }
  .bar i { display: block; height: 100%; background: linear-gradient(90deg, var(--cyan), var(--pink)); }

  /* ---------- toolbar ---------- */
  .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin: 18px 0 12px; }
  .search { position: relative; flex: 1 1 260px; }
  .search input {
    width: 100%; background: var(--card); border: 1px solid var(--line); color: var(--text); font: inherit; font-size: 15px;
    padding: 13px 16px 13px 44px; border-radius: 30px; outline: none; transition: border-color .15s, box-shadow .15s;
  }
  .search input:focus { border-color: var(--cyan); box-shadow: 0 0 0 3px rgba(25,227,255,.15); }
  .search svg { position: absolute; left: 15px; top: 50%; transform: translateY(-50%); width: 18px; height: 18px; color: var(--muted); }
  .tabs { display: flex; gap: 8px; flex-wrap: wrap; }
  .tab {
    background: var(--card); border: 1px solid var(--line); color: var(--muted); font-weight: 600; font-size: 13px;
    padding: 11px 16px; border-radius: 30px; cursor: pointer; transition: all .15s; white-space: nowrap;
  }
  .tab.active { color: var(--on-cyan); background: var(--cyan); border-color: var(--cyan); }
  .icon-btn {
    width: 42px; height: 42px; border-radius: 50%; background: var(--card); border: 1px solid var(--line); cursor: pointer;
    display: inline-flex; align-items: center; justify-content: center; font-size: 17px; flex-shrink: 0;
  }
  .icon-btn:hover { border-color: var(--cyan); }
  .pill-btn {
    height: 42px; padding: 0 16px; border-radius: 30px; background: var(--card); border: 1px solid var(--line); cursor: pointer;
    font-weight: 700; font-size: 13px; display: inline-flex; align-items: center; gap: 7px; white-space: nowrap;
  }
  .pill-btn:hover { border-color: var(--cyan); color: var(--cyan); }
  .pill-btn.install { border-color: var(--green); color: var(--green); }

  /* ---------- category chips ---------- */
  .chips-row { display: flex; gap: 8px; overflow-x: auto; padding: 2px 0 12px; scrollbar-width: none; }
  .chips-row::-webkit-scrollbar { display: none; }
  .cchip {
    flex: 0 0 auto; background: transparent; border: 1px solid var(--line); color: var(--text); font-weight: 600; font-size: 13px;
    padding: 8px 15px; border-radius: 30px; cursor: pointer; transition: all .15s; white-space: nowrap;
  }
  .cchip small { color: var(--muted); font-weight: 600; margin-left: 5px; }
  .cchip:hover { border-color: var(--pink); }
  .cchip.active { background: var(--pink); border-color: var(--pink); color: #fff; }
  .cchip.active small { color: rgba(255,255,255,.85); }
  .count { color: var(--muted); font-size: 12px; margin: 0 2px 10px; }

  /* ---------- grid of titles ---------- */
  .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; }
  @media (min-width: 560px) { .grid { grid-template-columns: repeat(3, 1fr); } }
  @media (min-width: 820px) { .grid { grid-template-columns: repeat(4, 1fr); gap: 18px; } }
  @media (min-width: 1040px) { .grid { grid-template-columns: repeat(5, 1fr); } }
  .card {
    display: block; background: var(--card); border: 1px solid var(--line); border-radius: var(--radius); overflow: hidden;
    cursor: pointer; transition: transform .18s ease, border-color .18s ease, box-shadow .18s ease; text-align: left;
    width: 100%;
  }
  .card:hover, .card:focus-visible { transform: translateY(-4px); border-color: var(--cyan); box-shadow: 0 10px 28px rgba(25,227,255,.14); outline: none; }
  .poster { position: relative; aspect-ratio: 2/3; background: linear-gradient(160deg, #1a2036, #0d101a); overflow: hidden; }
  .poster img { width: 100%; height: 100%; object-fit: cover; display: block; }
  .poster .ph {
    position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
    font-family: 'Orbitron', sans-serif; font-weight: 900; font-size: 44px; color: rgba(255,255,255,.16);
  }
  .badge-rating {
    position: absolute; top: 8px; right: 8px; background: rgba(7,8,15,.85); color: #ffd166; font-weight: 800; font-size: 12px;
    padding: 3px 8px; border-radius: 12px; border: 1px solid rgba(255,209,102,.5);
  }
  .badge-count { position: absolute; top: 8px; left: 8px; background: var(--pink); color: #fff; font-weight: 800; font-size: 11px; padding: 3px 8px; border-radius: 12px; }
  .badge-cat {
    position: absolute; left: 6px; bottom: 34px; background: rgba(7,8,15,.82); color: var(--cyan); font-weight: 700; font-size: 10.5px;
    padding: 2px 8px; border-radius: 8px; border: 1px solid rgba(25,227,255,.4);
  }
  .poster .qs { position: absolute; left: 6px; right: 6px; bottom: 6px; display: flex; gap: 4px; flex-wrap: wrap; }
  .q-chip { background: rgba(7,8,15,.82); border: 1px solid rgba(255,255,255,.2); color: #fff; font-size: 10.5px; font-weight: 700; padding: 2px 7px; border-radius: 8px; }
  .card .info { padding: 10px 12px 12px; }
  .card .name {
    font-weight: 700; font-size: 14px; line-height: 1.3; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    overflow: hidden; word-break: break-word;
  }
  .card .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }

  .more { display: block; margin: 22px auto 0; background: var(--card); border: 1px solid var(--line); font-weight: 600; padding: 12px 26px; border-radius: 30px; cursor: pointer; }
  .more:hover { border-color: var(--cyan); }
  .empty { text-align: center; color: var(--muted); padding: 60px 10px; }

  /* ---------- saved list ---------- */
  .saved { display: flex; flex-direction: column; gap: 10px; }
  .saved .file .f-q { background: var(--pink); color: #fff; min-width: 44px; }

  /* ---------- detail view ---------- */
  .back {
    display: inline-flex; align-items: center; gap: 8px; background: var(--card); border: 1px solid var(--line);
    font-weight: 700; font-size: 14px; padding: 10px 18px; border-radius: 30px; cursor: pointer; margin: 18px 0 14px;
  }
  .back:hover { border-color: var(--cyan); color: var(--cyan); }
  .hero { position: relative; overflow: hidden; border-radius: 18px; border: 1px solid var(--line); background: #0b0d15; color: #eef1f8; isolation: isolate; margin-bottom: 18px; }
  .hero-bg { position: absolute; inset: 0; z-index: -2; background-size: cover; background-position: center top; transform: scale(1.05); }
  .hero::before { content: ""; position: absolute; inset: 0; z-index: -1; background: linear-gradient(90deg, rgba(7,8,15,.96) 0%, rgba(7,8,15,.82) 50%, rgba(7,8,15,.6) 100%); }
  .hero-in { display: flex; gap: 18px; padding: 18px; align-items: flex-start; }
  .hero-poster { width: 128px; flex-shrink: 0; aspect-ratio: 2/3; object-fit: cover; border-radius: 10px; border: 1px solid rgba(255,255,255,.18); box-shadow: 0 10px 28px rgba(0,0,0,.55); background: #1a2036; }
  .hero-ph { width: 128px; flex-shrink: 0; aspect-ratio: 2/3; border-radius: 10px; background: linear-gradient(160deg,#1a2036,#0d101a); display: flex; align-items: center; justify-content: center; font-family: 'Orbitron', sans-serif; font-weight: 900; font-size: 40px; color: rgba(255,255,255,.2); }
  .hero-body { min-width: 0; flex: 1; display: flex; flex-direction: column; gap: 10px; }
  .hero-title { font-size: 22px; font-weight: 800; line-height: 1.2; word-break: break-word; }
  .hero-title small { font-weight: 500; opacity: .65; font-size: 17px; margin-left: 6px; }
  .rate-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .ring { --c: #21d07a; width: 50px; height: 50px; border-radius: 50%; display: flex; align-items: center; justify-content: center; position: relative; flex-shrink: 0;
    background: conic-gradient(var(--c) calc(var(--p) * 1%), rgba(255,255,255,.16) 0); }
  .ring::before { content: ""; position: absolute; inset: 5px; border-radius: 50%; background: #081c22; }
  .ring span { position: relative; font-weight: 800; font-size: 14px; }
  .ring.mid { --c: #d2d531; } .ring.low { --c: #db2360; }
  .rate-txt { font-size: 12px; opacity: .8; line-height: 1.35; }
  .rate-txt b { display: block; font-size: 13px; }
  .chips { display: flex; flex-wrap: wrap; gap: 6px; }
  .chip { font-size: 11.5px; font-weight: 600; padding: 4px 10px; border-radius: 20px; background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.16); }
  .overview { font-size: 13.5px; line-height: 1.6; opacity: .92; }
  .btn { display: inline-flex; align-items: center; gap: 7px; font-size: 12.5px; font-weight: 700; padding: 8px 14px; border-radius: 10px; color: #fff; background: rgba(255,255,255,.14); border: 1px solid rgba(255,255,255,.22); }
  .btn.red { background: #e5383b; border-color: #e5383b; }
  .plain-hero { padding: 18px; display: flex; flex-direction: column; gap: 8px; background: var(--card); color: var(--text); }
  .plain-hero h1 { font-size: 22px; line-height: 1.25; word-break: break-word; }
  .plain-hero p { color: var(--muted); font-size: 13px; }

  .files-h { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin: 6px 2px 10px; }
  .files-h h2 { font-size: 16px; }
  .files-h span { color: var(--muted); font-size: 12.5px; }
  .files { display: flex; flex-direction: column; gap: 10px; }
  .file {
    display: flex; align-items: center; gap: 12px; background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 12px 14px;
    transition: border-color .15s, transform .15s, background .15s;
  }
  .file:hover, .file:focus-visible { border-color: var(--cyan); background: var(--card2); transform: translateX(3px); outline: none; }
  .f-q { flex-shrink: 0; min-width: 58px; text-align: center; font-weight: 800; font-size: 13px; color: var(--on-cyan); background: var(--cyan); padding: 6px 8px; border-radius: 9px; }
  .f-q.ep { background: #ffd166; color: #14101d; }
  .f-main { min-width: 0; flex: 1; }
  .f-name { font-size: 13px; line-height: 1.4; word-break: break-word; }
  .f-meta { font-size: 12px; color: var(--muted); margin-top: 3px; }
  .f-meta .ok { color: var(--green); font-weight: 700; }
  .f-go { flex-shrink: 0; width: 38px; height: 38px; border-radius: 50%; background: var(--pink); color: #fff; display: flex; align-items: center; justify-content: center; font-size: 15px; }

  /* ---------- floating buttons + modal ---------- */
  .fab {
    position: fixed; right: 16px; bottom: 18px; z-index: 50; width: 54px; height: 54px; border-radius: 50%; background: #25d366; color: #fff;
    display: flex; align-items: center; justify-content: center; box-shadow: 0 8px 22px rgba(37,211,102,.45); transition: transform .15s;
  }
  .fab:hover { transform: scale(1.07); }
  .fab svg { width: 28px; height: 28px; fill: #fff; }
  .modal-bg {
    position: fixed; inset: 0; z-index: 100; background: rgba(0,0,0,.6); display: flex; align-items: center; justify-content: center; padding: 16px;
  }
  .modal { width: 100%; max-width: 420px; background: var(--bg2); border: 1px solid var(--line); border-radius: 16px; padding: 20px; }
  .modal h3 { margin-bottom: 4px; }
  .modal p.s { color: var(--muted); font-size: 12.5px; margin-bottom: 12px; }
  .modal input, .modal textarea {
    width: 100%; background: var(--card); border: 1px solid var(--line); color: var(--text); font: inherit; font-size: 14px;
    padding: 11px 13px; border-radius: 10px; outline: none; margin-bottom: 10px; resize: vertical;
  }
  .modal input:focus, .modal textarea:focus { border-color: var(--cyan); }
  .modal .row { display: flex; gap: 10px; justify-content: flex-end; align-items: center; }
  .modal .status { flex: 1; font-size: 12.5px; }
  .modal .go { background: var(--pink); color: #fff; border: 0; font-weight: 700; padding: 10px 20px; border-radius: 10px; cursor: pointer; }
  .modal .x { background: transparent; border: 1px solid var(--line); padding: 10px 16px; border-radius: 10px; cursor: pointer; }

  footer { text-align: center; color: var(--muted); font-size: 12px; margin-top: 34px; padding: 0 16px; }

  @media (max-width: 640px) {
    .hero-in { flex-direction: column; align-items: center; text-align: center; }
    .rate-row, .chips { justify-content: center; }
    .hero::before { background: linear-gradient(180deg, rgba(7,8,15,.8), rgba(7,8,15,.95) 60%); }
    .pill-btn .lbl { display: none; }
    .pill-btn { width: 42px; padding: 0; justify-content: center; }
  }
</style>
</head>
<body>

<header>
  <div class="logo" id="logo">AstraToonix</div>
  <div class="tagline">Stream the Universe</div>
</header>

<div class="notice" id="notice" hidden>
  <div class="notice-in">
    <span class="ico">📢</span>
    <span class="txt" id="noticeText"></span>
    <button id="noticeX" type="button" aria-label="Close">✕</button>
  </div>
</div>

{{if .AdEnabled}}
<div class="ad-zone">
  <div class="ad-top">
    <div class="ad-badge">Advertise</div>
    {{if .AdLinkURL}}<a href="{{.AdLinkURL}}" target="_blank" rel="noopener sponsored" style="display:block;">{{end}}
    {{if eq .AdType "video"}}<video src="{{.AdURL}}" autoplay muted loop playsinline></video>{{else}}<img src="{{.AdURL}}" alt="Advertise">{{end}}
    {{if .AdLinkURL}}</a>{{end}}
  </div>
</div>
{{end}}

{{if .AdSlotHTML}}
<div class="ad-zone"><div class="ad-slot">{{.AdSlotHTML}}</div></div>
{{end}}

<div class="wrap">

  <!-- ===== LIST: one card per title ===== -->
  <section id="listView">

    <div class="cw" id="cw" hidden>
      <div class="sec-h"><h2>▶ Continue Watching</h2><button id="cwClear" type="button">Clear</button></div>
      <div class="cw-row" id="cwRow"></div>
    </div>

    <div class="toolbar">
      <label class="search">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
        <input id="q" type="search" placeholder="Search videos, lectures, movies..." autocomplete="off" enterkeyhint="search">
      </label>
      <button class="pill-btn" id="msgBtn" type="button" title="Message / Doubt">💬 <span class="lbl">Message</span></button>
      <button class="pill-btn install" id="installBtn" type="button" hidden title="Install app">📲 <span class="lbl">Install</span></button>
      <button class="icon-btn" id="themeBtn" type="button" title="Light / Dark">🌓</button>
    </div>

    <div class="tabs" style="margin-bottom:12px">
      <button class="tab active" data-sort="new" type="button">🆕 New</button>
      <button class="tab" data-sort="popular" type="button">🔥 Popular</button>
      <button class="tab" data-sort="saved" type="button">❤ Saved</button>
    </div>

    <div class="chips-row" id="chips" hidden></div>
    <div class="count" id="count"></div>
    <div class="grid" id="grid"></div>
    <div class="saved" id="savedBox" hidden></div>
    <div class="empty" id="empty" hidden>Kuch nahi mila 😕</div>
    <button class="more" id="more" type="button" hidden>Load more</button>
    <div id="sentinel" style="height:1px"></div>
  </section>

  <!-- ===== DETAIL: all files of the clicked title ===== -->
  <section id="detailView" hidden>
    <button class="back" id="back" type="button">← Back</button>
    <div class="hero" id="hero"></div>
    <div class="files-h"><h2>Choose quality / file</h2><span id="fileCount"></span></div>
    <div class="files" id="files"></div>
  </section>

  {{if .AdLinkURL}}
  <div style="margin-top:22px">
    <a class="sponsor-strip" href="{{.AdLinkURL}}" target="_blank" rel="noopener sponsored">
      <span class="sponsor-tag">Sponsored</span>
      <span class="sponsor-text">{{.AdLinkText}}</span>
      <span>↗</span>
    </a>
  </div>
  {{end}}
</div>

<footer>© AstraToonix · Built by Raj Dev · home v3</footer>

{{if .WhatsAppURL}}
<a class="fab" href="{{.WhatsAppURL}}" target="_blank" rel="noopener" aria-label="WhatsApp" title="WhatsApp par poochho">
  <svg viewBox="0 0 32 32"><path d="M16.04 3C9.4 3 4 8.36 4 14.97c0 2.35.69 4.55 1.88 6.4L4 29l7.8-1.83a12.1 12.1 0 0 0 4.24.77C22.68 27.94 28 22.58 28 15.97 28 9.36 22.68 3 16.04 3Zm0 22.9c-1.3 0-2.56-.27-3.7-.78l-.27-.12-4.63 1.09 1.14-4.5-.17-.28a9.8 9.8 0 0 1-1.5-5.2c0-5.43 4.5-9.85 10.03-9.85 5.52 0 10.02 4.42 10.02 9.85 0 5.43-4.5 9.79-9.92 9.79Zm5.5-7.37c-.3-.15-1.78-.88-2.05-.98-.28-.1-.48-.15-.68.15-.2.3-.78.98-.95 1.18-.18.2-.35.22-.65.07-.3-.15-1.27-.47-2.42-1.5-.9-.8-1.5-1.78-1.67-2.08-.18-.3-.02-.46.13-.6.14-.13.3-.35.45-.52.15-.18.2-.3.3-.5.1-.2.05-.38-.03-.53-.07-.15-.68-1.63-.93-2.24-.25-.58-.5-.5-.68-.5l-.58-.01c-.2 0-.52.07-.8.38-.27.3-1.04 1.02-1.04 2.48 0 1.46 1.07 2.88 1.22 3.08.15.2 2.1 3.2 5.08 4.48.7.3 1.26.48 1.7.62.72.23 1.37.2 1.88.12.57-.08 1.78-.72 2.03-1.42.25-.7.25-1.3.18-1.42-.08-.12-.28-.2-.58-.35Z"/></svg>
</a>
{{end}}

<div class="modal-bg" id="msgModal" hidden>
  <div class="modal">
    <h3>💬 Message / Doubt</h3>
    <p class="s">Apna sawal ya message likho — seedha sir tak pahunchega.</p>
    <input id="msgName" type="text" maxlength="100" placeholder="Tumhara naam">
    <textarea id="msgText" rows="4" maxlength="1000" placeholder="Likho..."></textarea>
    <div class="row">
      <span class="status" id="msgStatus"></span>
      <button class="x" id="msgCancel" type="button">Cancel</button>
      <button class="go" id="msgSend" type="button">Bhejo</button>
    </div>
  </div>
</div>

<script>
(function () {
  var $ = function (id) { return document.getElementById(id); };
  var grid = $('grid'), more = $('more'), emptyEl = $('empty'), countEl = $('count');
  var listView = $('listView'), detailView = $('detailView'), chipsEl = $('chips'), savedBox = $('savedBox');
  var state = { q: '', sort: 'new', cat: '', page: 0, hasMore: false, loading: false, token: 0 };
  var savedScroll = 0;
  var CW_KEY = 'atx-cw', WL_KEY = 'raj-stream-watchlist';

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function jget(key) { try { return JSON.parse(localStorage.getItem(key)) || []; } catch (e) { return []; } }
  function fmtTime(s) {
    s = Math.max(0, Math.floor(s));
    var h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60;
    return (h ? h + ':' + (m < 10 ? '0' : '') : '') + m + ':' + (x < 10 ? '0' : '') + x;
  }
  function cleanName(n) { return String(n || '').replace(/\.(mkv|mp4|avi|mov|webm|m4v|ts)$/i, '').replace(/[._]+/g, ' ').trim(); }

  /* ---------------- theme ---------------- */
  function applyTheme(t) {
    document.documentElement.setAttribute('data-theme', t);
    var m = document.querySelector('meta[name=theme-color]');
    if (m) m.setAttribute('content', t === 'light' ? '#f3f5fb' : '#07080f');
  }
  try { applyTheme(localStorage.getItem('atx-home-theme') === 'light' ? 'light' : 'dark'); } catch (e) {}
  $('themeBtn').addEventListener('click', function () {
    var t = document.documentElement.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
    applyTheme(t);
    try { localStorage.setItem('atx-home-theme', t); } catch (e) {}
  });

  /* ---------------- notice (/reply from Telegram) ---------------- */
  function pollNotice() {
    fetch('/announcement', { cache: 'no-store' }).then(function (r) { return r.json(); }).then(function (d) {
      var box = $('notice');
      if (!d || !d.message) { box.hidden = true; return; }
      var dismissed = null;
      try { dismissed = localStorage.getItem('atx-home-notice-x'); } catch (e) {}
      if (String(d.updated_at) === dismissed) { box.hidden = true; return; }
      $('noticeText').textContent = d.message;
      box.setAttribute('data-ts', d.updated_at);
      box.hidden = false;
    }).catch(function () {});
  }
  $('noticeX').addEventListener('click', function () {
    var box = $('notice');
    try { localStorage.setItem('atx-home-notice-x', box.getAttribute('data-ts') || ''); } catch (e) {}
    box.hidden = true;
  });
  pollNotice();
  setInterval(pollNotice, 30000);

  /* ---------------- continue watching ---------------- */
  function renderContinue() {
    var items = jget(CW_KEY).filter(function (x) { return x && x.id && !x.done && x.t > 5 && x.d > 0; })
      .sort(function (a, b) { return b.ts - a.ts; }).slice(0, 10);
    var box = $('cw'), row = $('cwRow');
    row.innerHTML = '';
    if (!items.length) { box.hidden = true; return; }
    items.forEach(function (it) {
      var a = el('a', 'cw-card');
      a.href = '/watch/' + encodeURIComponent(it.id);
      a.appendChild(el('div', 'cw-name', cleanName(it.name)));
      var meta = el('div', 'cw-meta');
      meta.appendChild(el('span', null, fmtTime(it.d - it.t) + ' left'));
      meta.appendChild(el('span', 'cw-play', '▶ Resume'));
      a.appendChild(meta);
      var bar = el('div', 'bar'), i = el('i');
      i.style.width = Math.min(100, Math.round(it.t / it.d * 100)) + '%';
      bar.appendChild(i); a.appendChild(bar);
      row.appendChild(a);
    });
    box.hidden = false;
  }
  $('cwClear').addEventListener('click', function () {
    try { localStorage.removeItem(CW_KEY); } catch (e) {}
    renderContinue();
  });

  /* ---------------- category chips ---------------- */
  function renderChips(cats) {
    chipsEl.innerHTML = '';
    if (!cats || cats.length < 2 || state.sort === 'saved') { chipsEl.hidden = true; return; }
    var total = 0; cats.forEach(function (c) { total += c.count; });
    var all = [{ name: '', label: 'All', count: total }].concat(cats.map(function (c) { return { name: c.name, label: c.name, count: c.count }; }));
    all.forEach(function (c) {
      var b = el('button', 'cchip' + (state.cat === c.name ? ' active' : ''), c.label);
      b.type = 'button';
      b.appendChild(el('small', null, String(c.count)));
      b.addEventListener('click', function () { state.cat = c.name; load(true); });
      chipsEl.appendChild(b);
    });
    chipsEl.hidden = false;
  }

  /* ---------------- list ---------------- */
  function card(it) {
    var b = el('button', 'card');
    b.type = 'button';
    var poster = el('div', 'poster');
    poster.appendChild(el('div', 'ph', (it.title || '?').charAt(0).toUpperCase()));
    if (it.poster) {
      var img = el('img');
      img.loading = 'lazy'; img.alt = ''; img.src = it.poster;
      img.onerror = function () { img.remove(); };
      poster.appendChild(img);
    }
    if (it.count > 1) poster.appendChild(el('span', 'badge-count', it.count + ' files'));
    if (it.rating) poster.appendChild(el('span', 'badge-rating', '⭐ ' + Number(it.rating).toFixed(1)));
    if (it.category && it.category !== 'Other') poster.appendChild(el('span', 'badge-cat', it.category));
    if (it.qualities && it.qualities.length) {
      var qs = el('div', 'qs');
      it.qualities.forEach(function (q) { qs.appendChild(el('span', 'q-chip', q)); });
      poster.appendChild(qs);
    }
    var info = el('div', 'info');
    info.appendChild(el('div', 'name', it.title));
    var sub = [];
    if (it.year) sub.push(it.year);
    sub.push(it.count + (it.count === 1 ? ' file' : ' files'));
    info.appendChild(el('div', 'sub', sub.join(' · ')));
    b.appendChild(poster); b.appendChild(info);
    b.addEventListener('click', function () { openTitle(it.key, true); });
    return b;
  }

  function showSaved() {
    grid.hidden = true; more.hidden = true; emptyEl.hidden = true; chipsEl.hidden = true;
    savedBox.hidden = false; savedBox.innerHTML = '';
    var items = jget(WL_KEY);
    countEl.textContent = items.length ? items.length + ' saved' : '';
    if (!items.length) {
      savedBox.appendChild(el('div', 'empty', 'Abhi kuch save nahi hai. Kisi video ke page par bookmark dabao.'));
      return;
    }
    items.forEach(function (it) {
      var a = el('a', 'file');
      a.href = it.url;
      a.appendChild(el('div', 'f-q', '❤'));
      var main = el('div', 'f-main');
      main.appendChild(el('div', 'f-name', cleanName(it.name)));
      a.appendChild(main);
      a.appendChild(el('div', 'f-go', '▶'));
      savedBox.appendChild(a);
    });
  }

  function load(reset) {
    if (state.sort === 'saved') { showSaved(); return; }
    savedBox.hidden = true; grid.hidden = false;
    if (state.loading && !reset) return;
    if (reset) { state.page = 0; state.token++; grid.innerHTML = ''; state.loading = false; }
    state.loading = true;
    var tok = state.token;
    var url = '/api/titles?page=' + state.page + '&sort=' + state.sort + '&q=' + encodeURIComponent(state.q) + '&cat=' + encodeURIComponent(state.cat);
    fetch(url, { cache: 'no-store' }).then(function (r) { return r.json(); }).then(function (d) {
      if (tok !== state.token) return;
      (d.items || []).forEach(function (it) { grid.appendChild(card(it)); });
      state.hasMore = !!d.has_more;
      state.page++;
      emptyEl.hidden = grid.children.length > 0;
      countEl.textContent = grid.children.length ? (d.total + (d.total === 1 ? ' title' : ' titles')) : '';
      more.hidden = !state.hasMore;
      renderChips(d.cats);
    }).catch(function () {
      if (tok !== state.token) return;
      countEl.textContent = 'Load nahi ho paya — dobara try karo';
      more.hidden = false; more.textContent = 'Retry';
    }).then(function () { if (tok === state.token) state.loading = false; });
  }

  more.addEventListener('click', function () { more.textContent = 'Load more'; load(false); });
  if ('IntersectionObserver' in window) {
    new IntersectionObserver(function (es) {
      if (es[0].isIntersecting && state.hasMore && !state.loading && !listView.hidden && state.sort !== 'saved') load(false);
    }, { rootMargin: '600px' }).observe($('sentinel'));
  }

  var qTimer;
  $('q').addEventListener('input', function (e) {
    clearTimeout(qTimer);
    var v = e.target.value.trim();
    qTimer = setTimeout(function () {
      state.q = v;
      if (state.sort === 'saved') { setSort('new'); }
      load(true);
    }, 250);
  });
  function setSort(s) {
    state.sort = s;
    Array.prototype.forEach.call(document.querySelectorAll('.tab'), function (x) {
      x.classList.toggle('active', x.getAttribute('data-sort') === s);
    });
  }
  Array.prototype.forEach.call(document.querySelectorAll('.tab'), function (t) {
    t.addEventListener('click', function () { setSort(t.getAttribute('data-sort')); load(true); });
  });
  $('logo').addEventListener('click', function () { showList(); window.scrollTo(0, 0); });

  /* ---------------- detail ---------------- */
  function showList() {
    detailView.hidden = true;
    listView.hidden = false;
    renderContinue();
    if (location.hash) history.replaceState(null, '', location.pathname + location.search);
    window.scrollTo(0, savedScroll);
  }

  function ringClass(p) { return p >= 70 ? '' : (p >= 50 ? ' mid' : ' low'); }

  function renderHero(d, meta) {
    var hero = $('hero');
    hero.innerHTML = '';
    meta = meta && meta.title ? meta : null;
    var posterUrl = (meta && meta.poster_url) || d.poster;
    var rating = (meta && meta.rating) || d.rating || 0;
    if (!meta && !posterUrl && !rating) {
      // coaching video / no TMDB info: a clean text header
      var ph = el('div', 'plain-hero');
      ph.appendChild(el('h1', null, d.title + (d.year ? ' (' + d.year + ')' : '')));
      if (d.category) ph.appendChild(el('p', null, d.category + ' · ' + (d.files || []).length + ' file(s)'));
      hero.appendChild(ph);
      return;
    }
    if (meta && meta.backdrop_url) {
      var bg = el('div', 'hero-bg');
      bg.style.backgroundImage = "url('" + meta.backdrop_url + "')";
      hero.appendChild(bg);
    }
    var inner = el('div', 'hero-in');
    if (posterUrl) {
      var im = el('img', 'hero-poster'); im.alt = ''; im.src = posterUrl; im.onerror = function () { im.remove(); };
      inner.appendChild(im);
    } else {
      inner.appendChild(el('div', 'hero-ph', (d.title || '?').charAt(0).toUpperCase()));
    }
    var body = el('div', 'hero-body');
    var t = el('div', 'hero-title', d.title);
    if (d.year) t.appendChild(el('small', null, '(' + d.year + ')'));
    body.appendChild(t);
    if (rating) {
      var pct = Math.round(rating * 10);
      var row = el('div', 'rate-row');
      var ring = el('div', 'ring' + ringClass(pct));
      ring.style.setProperty('--p', pct);
      ring.appendChild(el('span', null, Number(rating).toFixed(1)));
      row.appendChild(ring);
      var rt = el('div', 'rate-txt');
      rt.appendChild(el('b', null, 'User Score'));
      rt.appendChild(document.createTextNode(meta && meta.votes ? Number(meta.votes).toLocaleString() + ' votes on TMDB' : 'TMDB'));
      row.appendChild(rt);
      body.appendChild(row);
    }
    if (meta) {
      var chips = el('div', 'chips');
      chips.appendChild(el('span', 'chip', meta.media_type === 'tv' ? '📺 TV Series' : '🎬 Movie'));
      if (meta.runtime) {
        var h = Math.floor(meta.runtime / 60), m = meta.runtime % 60;
        chips.appendChild(el('span', 'chip', '⏱ ' + (meta.media_type === 'tv' ? meta.runtime + ' min/ep' : (h ? h + 'h ' : '') + (m ? m + 'm' : ''))));
      }
      if (meta.seasons) chips.appendChild(el('span', 'chip', meta.seasons + ' Season' + (meta.seasons > 1 ? 's' : '')));
      (meta.genres || []).forEach(function (g) { chips.appendChild(el('span', 'chip', g)); });
      body.appendChild(chips);
      if (meta.overview) body.appendChild(el('div', 'overview', meta.overview));
      var acts = el('div', 'chips');
      if (meta.trailer_key) {
        var tr = el('a', 'btn red', '▶ Trailer');
        tr.target = '_blank'; tr.rel = 'noopener';
        tr.href = 'https://www.youtube.com/watch?v=' + encodeURIComponent(meta.trailer_key);
        acts.appendChild(tr);
      }
      if (meta.source_url) {
        var tm = el('a', 'btn', 'View on TMDB ↗');
        tm.target = '_blank'; tm.rel = 'noopener'; tm.href = meta.source_url;
        acts.appendChild(tm);
      }
      if (acts.children.length) body.appendChild(acts);
    }
    inner.appendChild(body);
    hero.appendChild(inner);
  }

  function renderFiles(files) {
    var box = $('files');
    box.innerHTML = '';
    $('fileCount').textContent = files.length + (files.length === 1 ? ' file' : ' files');
    var seen = {};
    jget(CW_KEY).forEach(function (x) { if (x && x.id) seen[x.id] = x; });
    files.forEach(function (f) {
      var a = el('a', 'file');
      a.href = '/watch/' + encodeURIComponent(f.id);
      var q = el('div', 'f-q' + (f.episode ? ' ep' : ''), f.episode || f.quality);
      var main = el('div', 'f-main');
      main.appendChild(el('div', 'f-name', f.name));
      var meta = el('div', 'f-meta');
      var parts = [];
      if (f.episode) parts.push(f.quality);
      parts.push(f.size);
      if (f.locked) parts.push('🔒 password');
      meta.textContent = parts.join(' · ');
      var rec = seen[f.id];
      if (rec && rec.done) {
        meta.appendChild(document.createTextNode(' · '));
        meta.appendChild(el('span', 'ok', '✔ Watched'));
      } else if (rec && rec.t > 5 && rec.d > 0) {
        meta.appendChild(document.createTextNode(' · Resume ' + fmtTime(rec.t)));
      }
      main.appendChild(meta);
      if (rec && !rec.done && rec.t > 5 && rec.d > 0) {
        var bar = el('div', 'bar'), i = el('i');
        i.style.width = Math.min(100, Math.round(rec.t / rec.d * 100)) + '%';
        bar.appendChild(i); main.appendChild(bar);
      }
      a.appendChild(q); a.appendChild(main);
      a.appendChild(el('div', 'f-go', '▶'));
      box.appendChild(a);
    });
  }

  function openTitle(key, push) {
    if (listView.hidden === false) savedScroll = window.pageYOffset;
    listView.hidden = true;
    detailView.hidden = false;
    window.scrollTo(0, 0);
    $('hero').innerHTML = '';
    $('files').innerHTML = '<div class="empty">Loading...</div>';
    $('fileCount').textContent = '';
    if (push) history.pushState({ key: key, fromList: true }, '', '#/t/' + key);
    fetch('/api/title/' + encodeURIComponent(key), { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error('nf');
      return r.json();
    }).then(function (d) {
      document.title = d.title + ' — AstraToonix';
      renderHero(d, null);
      renderFiles(d.files || []);
      if (d.meta_file_id) {
        fetch('/meta/' + encodeURIComponent(d.meta_file_id), { cache: 'no-store' }).then(function (r) { return r.json(); })
          .then(function (m) { if (m && m.title && !detailView.hidden) renderHero(d, m); }).catch(function () {});
      }
    }).catch(function () {
      $('files').innerHTML = '<div class="empty">Ye title ab nahi mila. Back jaakar dobara try karo.</div>';
    });
  }

  $('back').addEventListener('click', function () {
    document.title = 'AstraToonix — Stream the Universe';
    if (history.state && history.state.fromList) history.back();
    else showList();
  });

  window.addEventListener('popstate', function () {
    var m = /^#\/t\/([0-9a-f]+)$/.exec(location.hash);
    if (m) openTitle(m[1], false);
    else { document.title = 'AstraToonix — Stream the Universe'; showList(); }
  });
  // coming back from a watch page (bfcache): refresh progress info
  window.addEventListener('pageshow', function (e) {
    if (e.persisted) { renderContinue(); if (!detailView.hidden) { var m = /^#\/t\/([0-9a-f]+)$/.exec(location.hash); if (m) openTitle(m[1], false); } }
  });

  /* ---------------- message / doubt ---------------- */
  var msgModal = $('msgModal');
  $('msgBtn').addEventListener('click', function () { msgModal.hidden = false; $('msgText').focus(); });
  function closeMsg() { msgModal.hidden = true; }
  $('msgCancel').addEventListener('click', closeMsg);
  msgModal.addEventListener('click', function (e) { if (e.target === msgModal) closeMsg(); });
  $('msgSend').addEventListener('click', function () {
    var st = $('msgStatus'), text = $('msgText').value.trim();
    if (!text) { st.textContent = 'Pehle message likho.'; st.style.color = '#f66'; return; }
    st.textContent = 'Bhej raha hun...'; st.style.color = '';
    fetch('/contact', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: $('msgName').value.trim(), message: text }) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (d) { if (!r.ok) throw new Error(d.error || 'Send nahi hua'); }); })
      .then(function () {
        st.textContent = '✅ Bhej diya gaya!'; st.style.color = '#21d07a';
        $('msgText').value = '';
        setTimeout(function () { closeMsg(); st.textContent = ''; }, 1200);
      })
      .catch(function (err) { st.textContent = '❌ ' + err.message; st.style.color = '#f66'; });
  });

  /* ---------------- install as app (PWA) ---------------- */
  var deferredInstall = null;
  window.addEventListener('beforeinstallprompt', function (e) {
    e.preventDefault(); deferredInstall = e; $('installBtn').hidden = false;
  });
  $('installBtn').addEventListener('click', function () {
    if (!deferredInstall) return;
    deferredInstall.prompt();
    deferredInstall.userChoice.then(function () { deferredInstall = null; $('installBtn').hidden = true; });
  });
  window.addEventListener('appinstalled', function () { $('installBtn').hidden = true; });
  if ('serviceWorker' in navigator) { navigator.serviceWorker.register('/sw.js').catch(function () {}); }

  /* ---------------- start ---------------- */
  renderContinue();
  load(true);
  var first = /^#\/t\/([0-9a-f]+)$/.exec(location.hash);
  if (first) openTitle(first[1], false);
})();
</script>
{{if .AdBodyHTML}}{{.AdBodyHTML}}{{end}}
</body>
</html>
`

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
	AccessID    string
	Slug        string

	ReferralLink  string
	ReferralCount int

	AdEnabled bool
	AdType    string
	AdURL     string

	// Sponsor link (/adlink) — makes the banner clickable and shows a
	// "Sponsored" button under the player.
	AdLinkURL  string
	AdLinkText string

	// TMDB snapshot as JSON for the page's script ("null" when none).
	TMDBJSON template.JS
	OGImage   string
	OGDesc    string

	// Owner-supplied ad code from env (trusted; deliberately not escaped).
	AdSlotHTML template.HTML
	AdBodyHTML template.HTML

	QualitiesJSON template.JS
}

// qualityOption is one entry in the watch page's quality switcher dropdown.
type qualityOption struct {
	Label  string `json:"label"`
	URL    string `json:"url"`
	Active bool   `json:"active"`
	Size   string `json:"size"`
	Name   string `json:"name"`
}

func renderSplash(w http.ResponseWriter, aboutText, nextURL string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
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

// resolvePromptVideoURL prefers a live override set via /video on Telegram
// (stored in Redis, no restart needed); falls back to the PASSWORD_PROMPT_VIDEO_URL
// env var, and to "" (no video shown) if neither is set.
func (a *App) resolvePromptVideoURL(ctx context.Context) string {
	if url, ok := a.cache.getPromptVideo(ctx); ok {
		return url
	}
	return a.cfg.PasswordPromptVideoURL
}

// resolveReferralWelcomeSlug prefers a live override set via /video2 on
// Telegram (no restart needed); falls back to REFERRAL_WELCOME_SLUG / its
// built-in default.
func (a *App) resolveReferralWelcomeSlug(ctx context.Context) string {
	if slug, ok := a.cache.getReferralWelcomeSlug(ctx); ok {
		return slug
	}
	return a.cfg.ReferralWelcomeSlug
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
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
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

// renderBlocked shows a blocked visitor their Access ID and a direct
// Telegram link to the admin, so they can actually request an unblock
// instead of just hitting a dead "403 Forbidden" with no way forward.
func renderBlocked(w http.ResponseWriter, accessID int, contactTelegram string) {
	tgUser := contactTelegram
	if tgUser == "" {
		tgUser = "raj_dev_01"
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.WriteHeader(http.StatusForbidden)
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Access Blocked</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
  body{background:#0b0d12;color:#e5e7eb;font-family:system-ui,-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:20px;}
  .card{background:#161a22;border:1px solid #2a2f3a;border-radius:16px;padding:28px;max-width:380px;width:100%%;text-align:center;}
  .icon{font-size:40px;margin-bottom:12px;}
  h1{font-size:19px;margin:0 0 8px;color:#f87171;}
  p{color:#9ca3af;font-size:13.5px;line-height:1.6;margin:0 0 18px;}
  .idbox{padding:14px;border-radius:10px;background:#0d1322;border:1px solid #1a2436;margin-bottom:18px;}
  .idbox .l{font-size:10.5px;letter-spacing:.5px;color:#6b7bab;margin-bottom:4px;}
  .idbox .n{font-family:monospace;font-size:24px;font-weight:800;color:#ffd166;letter-spacing:3px;}
  .tg-btn{display:flex;align-items:center;justify-content:center;gap:8px;padding:13px;border-radius:12px;background:#229ed9;color:#fff;text-decoration:none;font-weight:700;font-size:14px;}
</style></head><body>
<div class="card">
  <div class="icon">🚫</div>
  <h1>Access Blocked</h1>
  <p>Is device ko access se block kiya gaya hai. Agar tumhe lagta hai yeh galti se hua hai, apna Access ID batakar admin se unblock karne ki request karo.</p>
  <div class="idbox">
    <div class="l">YOUR ACCESS ID</div>
    <div class="n">%05d</div>
  </div>
  <a class="tg-btn" href="https://t.me/%s" target="_blank" rel="noopener">
    <i class="fa-brands fa-telegram"></i> Message Admin to Unblock
  </a>
</div>
</body></html>`, accessID, tgUser)
}

// promptVideoHTML is the autoplaying (muted, with a tap-for-sound button)
// video block shown on the password page and on the first-visit
// "waiting for approval" page.
func promptVideoHTML(videoURL string) string {
	return fmt.Sprintf(`
<div style="position:relative;margin:14px 0 16px;border-radius:12px;overflow:hidden;
box-shadow:0 0 0 1px rgba(56,214,255,.25),0 8px 24px rgba(0,0,0,.5);">
<video id="ppv" src="%s" autoplay muted loop playsinline
style="width:100%%;display:block;background:#000;"></video>
<button type="button" onclick="var v=document.getElementById('ppv');v.muted=!v.muted;this.textContent=v.muted?'🔇 Tap for sound':'🔊 Sound on';"
style="position:absolute;bottom:10px;right:10px;background:rgba(6,8,14,.75);color:#fff;
border:1px solid rgba(255,255,255,.25);backdrop-filter:blur(4px);
border-radius:20px;padding:6px 14px;font-size:12.5px;font-weight:600;cursor:pointer;">🔇 Tap for sound</button>
</div>`, html.EscapeString(videoURL))
}

// bufferedRW lets us post-process a page that a render function writes
// straight to the ResponseWriter (headers still go to the real writer).
type bufferedRW struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (b *bufferedRW) Write(p []byte) (int, error) { return b.buf.Write(p) }

// renderPendingApproval shows the "waiting for approval" page and, when a
// prompt video is configured (/video), plays it at the top — same video as
// the password page, so first-time visitors of ANY link see it.
func renderPendingApproval(w http.ResponseWriter, slug string, accessID int, prefillName, contactTelegram, contactInstagram, videoURL string) {
	if videoURL == "" {
		renderPendingApprovalBase(w, slug, accessID, prefillName, contactTelegram, contactInstagram)
		return
	}
	bw := &bufferedRW{ResponseWriter: w}
	renderPendingApprovalBase(bw, slug, accessID, prefillName, contactTelegram, contactInstagram)
	page := strings.Replace(bw.buf.String(), `<div class="spin"></div>`, promptVideoHTML(videoURL)+`<div class="spin"></div>`, 1)
	w.Write([]byte(page))
}

func renderPendingApprovalBase(w http.ResponseWriter, slug string, accessID int, prefillName, contactTelegram, contactInstagram string) {
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
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
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
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Query().Get("peek") == "1" {
		// Read-only check used by /redeem: never creates an approval record,
		// and premium counts as approved (premium skips approval anyway).
		rec, pErr := a.db.findApprovalByDevice(ctx, deviceID)
		approved, blocked := false, false
		if pErr == nil && rec != nil {
			approved, blocked = rec.Approved, rec.Blocked
		}
		if a.db.isPremium(ctx, deviceID) {
			approved = true
		}
		fmt.Fprintf(w, `{"approved":%t,"blocked":%t}`, approved, blocked)
		return
	}
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
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

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
			if ref := r.URL.Query().Get("ref"); ref != "" {
				http.Redirect(w, r, fmt.Sprintf("%s/redeem?ref=%s", a.cfg.baseURL(), url.QueryEscape(ref)), http.StatusFound)
				return
			}
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

	_, hadDeviceCookie := r.Cookie(deviceCookieName)
	deviceID := getOrSetDeviceID(w, r)
	isPrem := a.db.isPremium(ctx, deviceID)

	// Referral tracking: only for genuinely first-time visitors (device
	// cookie didn't exist before this request), so returning visitors who
	// happen to reopen a ?ref= link never get double-counted.
	if hadDeviceCookie != nil {
		if refCode := strings.TrimSpace(r.URL.Query().Get("ref")); refCode != "" {
			if refAccessID, convErr := strconv.Atoi(refCode); convErr == nil {
				if refApproval, rErr := a.db.getApprovalByAccessID(ctx, refAccessID); rErr == nil && refApproval != nil {
					if _, justEarned, recErr := a.db.recordReferral(ctx, refApproval.DeviceID, deviceID); recErr == nil && justEarned {
						if _, gErr := a.db.grantPremiumDirect(ctx, refApproval.DeviceID, 1); gErr != nil {
							a.logger.Warn("referral reward grant failed", zap.Error(gErr))
						}
					}
				}
			}
		}
	}

	watchApproval, _, watchApprovalErr := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if watchApprovalErr != nil {
		a.logger.Warn("getOrCreateApproval failed (handleWatch) — Access ID/referral link will be blank for this request", zap.Error(watchApprovalErr))
	}
	if watchApproval != nil && watchApproval.Blocked {
		renderBlocked(w, watchApproval.AccessID, a.cfg.ContactTelegramUsername)
		return
	}

	if passwordHash != nil && *passwordHash != "" && !isPrem {
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
					renderPasswordPrompt(w, slug, true, accessID, visitorName, a.resolvePromptVideoURL(ctx), a.cfg.PasswordPromptImages, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername)
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
			renderPasswordPrompt(w, slug, false, accessID, visitorName, a.resolvePromptVideoURL(ctx), a.cfg.PasswordPromptImages, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername)
			return
		}
	}

	approval, isNewDevice, err := a.db.getOrCreateApproval(ctx, deviceID, slug)
	if err != nil {
		a.logger.Warn("approval lookup failed", zap.Error(err))
	}
	if !isPrem && (approval == nil || !approval.Approved) {
		accessID := 0
		visitorName := ""
		if approval != nil {
			accessID = approval.AccessID
			visitorName = approval.VisitorName
		}
		a.notifyNewAccessID(isNewDevice, accessID, fileName, visitorName)
		renderPendingApproval(w, slug, accessID, visitorName, a.cfg.ContactTelegramUsername, a.cfg.ContactInstagramUsername, a.resolvePromptVideoURL(ctx))
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
	if watchApproval != nil {
		data.AccessID = fmt.Sprintf("%05d", watchApproval.AccessID)
		data.ReferralLink = fmt.Sprintf("%s/watch/%s?ref=%05d", base, a.resolveReferralWelcomeSlug(ctx), watchApproval.AccessID)
		data.ReferralCount = a.db.getReferralCount(ctx, deviceID)
	}
	if ad := a.cache.getAdvertise(ctx); ad != nil {
		data.AdEnabled = true
		data.AdType = ad.Type
		data.AdURL = ad.URL
	}
	if l := a.cache.getAdLink(ctx); l != nil {
		data.AdLinkURL = l.URL
		data.AdLinkText = l.Text
	} else if a.cfg.AdDirectLink != "" {
		data.AdLinkURL = a.cfg.AdDirectLink
		data.AdLinkText = a.cfg.AdLinkText
		if data.AdLinkText == "" {
			data.AdLinkText = "Visit Sponsor"
		}
	}
	data.AdSlotHTML = template.HTML(a.cfg.AdBannerHTML)
	data.AdBodyHTML = template.HTML(a.cfg.AdBodyHTML)

	data.TMDBJSON = template.JS("null")
	data.OGDesc = "Watch " + fileName + " — on Astratoonix Education Platform"
	if rec, mErr := a.db.getFileByID(ctx, slug); mErr == nil && rec.TMDB != nil {
		if b, jErr := json.Marshal(rec.TMDB); jErr == nil {
			data.TMDBJSON = template.JS(b)
		}
		if rec.TMDB.BackdropURL != "" {
			data.OGImage = rec.TMDB.BackdropURL
		} else {
			data.OGImage = rec.TMDB.PosterURL
		}
		if rec.TMDB.Overview != "" {
			data.OGDesc = rec.TMDB.Overview
		}
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
					Size:   formatSize(m.FileSize),
					Name:   m.FileName,
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
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	if err := tmpl.Execute(w, data); err != nil {
		a.logger.Error("watch template execute failed", zap.Error(err))
	}
}

// handleContact lets a website visitor send a short message straight to the
// admin's Telegram, without needing to open Telegram themselves first —
// POST { "name": "...", "message": "..." } as JSON.
// handlePremiumRequest is called when a visitor picks a plan on the "Buy
// Premium" page. It records a pending request and immediately notifies the
// admin on Telegram (with an inline Approve button) so they know someone is
// about to pay and can watch for the payment to land.
func (a *App) handlePremiumRequest(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		PlanID string `json:"plan_id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	plan, ok := findPremiumPlan(body.PlanID)
	if !ok {
		http.Error(w, `{"error":"unknown plan"}`, http.StatusBadRequest)
		return
	}

	deviceID := getOrSetDeviceID(w, r)
	reqID := uuid.New().String()
	pr := &PremiumRequest{
		ID:        reqID,
		DeviceID:  deviceID,
		PlanID:    plan.ID,
		PlanLabel: plan.Label,
		Days:      plan.Days,
		Price:     fmt.Sprintf("₹%d", plan.Price),
	}
	if err := a.db.createPremiumRequest(ctx, pr); err != nil {
		a.logger.Error("createPremiumRequest failed", zap.Error(err))
		http.Error(w, `{"error":"failed to create request"}`, http.StatusInternalServerError)
		return
	}

	kb := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("✅ Approve & Activate", "premapprove:"+reqID),
	))
	accessIDLine := ""
	if approval, _, aErr := a.db.getOrCreateApproval(ctx, deviceID, ""); aErr == nil && approval != nil {
		accessIDLine = fmt.Sprintf("\n🔑 Access ID: %05d", approval.AccessID)
	}
	msg := tgbotapi.NewMessage(a.cfg.AdminID, fmt.Sprintf(
		"🔔 Premium request\nPlan: %s (%s)\nDevice: %s%s\n\nPayment aane ke baad approve karo — code auto-generate hoke visitor ke device pe turant activate ho jaayega.",
		plan.Label, pr.Price, deviceID, accessIDLine,
	))
	msg.ReplyMarkup = kb
	a.pool.next().Send(msg)

	b, _ := json.Marshal(map[string]any{"request_id": reqID})
	w.Write(b)
}

// handlePremiumStatus is polled by the "Buy Premium" page while waiting for
// admin approval. Once approved, it reports back so the page can show a
// success animation — no code entry needed by the visitor at all, since
// approval already auto-grants premium directly to their device.
func (a *App) handlePremiumStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	reqID := r.URL.Query().Get("req")
	if reqID == "" {
		http.Error(w, `{"error":"missing req"}`, http.StatusBadRequest)
		return
	}
	pr, err := a.db.getPremiumRequest(ctx, reqID)
	if err != nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	b, _ := json.Marshal(map[string]any{"status": pr.Status})
	w.Write(b)
}

// handleTeam returns the owner (always the account behind AdminID, shown
// separately as "Owner") plus the cosmetic list of team members added via
// /admin — purely for display, grants no real bot access.
func (a *App) handleTeam(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	members, err := a.db.listTeamMembers(ctx)
	if err != nil {
		a.logger.Warn("listTeamMembers failed", zap.Error(err))
	}
	type teamEntry struct {
		Name     string `json:"name"`
		ImageURL string `json:"image_url"`
	}
	admins := make([]teamEntry, 0, len(members))
	for _, m := range members {
		admins = append(admins, teamEntry{Name: m.Name, ImageURL: m.ImageURL})
	}
	b, _ := json.Marshal(map[string]any{
		"owner":  teamEntry{Name: a.cfg.OwnerName, ImageURL: a.cfg.OwnerImageURL},
		"admins": admins,
	})
	w.Write(b)
}

// handleFileQualities returns the quality-variant list for a given file ID
// as JSON (same shape as the QualitiesJSON baked into the initial page
// load) — used by the client-side "play a different file without reloading
// the page" flow (clicking a card from Trending/search/etc.) so the Quality
// switcher updates to reflect whichever file is now actually playing,
// instead of staying stuck on the qualities of the file the page originally
// loaded with.
func (a *App) handleFileQualities(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	id := strings.TrimPrefix(r.URL.Path, "/qualities/")
	if id == "" {
		http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
		return
	}

	rec, err := a.db.getFileByID(ctx, id)
	if err != nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	base := a.cfg.baseURL()
	opts := []qualityOption{}
	if rec.GroupID != nil && *rec.GroupID != "" {
		if members, gErr := a.db.listGroupFiles(ctx, *rec.GroupID); gErr == nil && len(members) > 1 {
			opts = make([]qualityOption, 0, len(members))
			for _, m := range members {
				label := m.QualityLabel
				if label == "" {
					label = "SD"
				}
				opts = append(opts, qualityOption{
					Label:  label,
					URL:    fmt.Sprintf("%s/stream/%s", base, m.ID),
					Active: m.ID == id,
					Size:   formatSize(m.FileSize),
					Name:   m.FileName,
				})
			}
		}
	}

	b, _ := json.Marshal(map[string]any{
		"qualities": opts,
		"stream_url": fmt.Sprintf("%s/stream/%s", base, rec.ID),
		"download_url": fmt.Sprintf("%s/dl/%s", base, rec.ID),
		"watch_url": fmt.Sprintf("%s/watch/%s", base, rec.ID),
		"name": rec.FileName,
		"size": formatSize(rec.FileSize),
	})
	w.Write(b)
}

// handleContactCheck is polled by the website to see if the admin has
// replied (via /reply) to this specific browser's messages — this is the
// only way a reply can "reach" a visitor, since there's no persistent
// connection to their browser otherwise.
// handleAnnouncement is polled by every visitor's page to check for a
// site-wide banner message (set via /reply on Telegram) — global, not tied
// to any specific device, so it's guaranteed to reach everyone regardless
// of cookies/sessions.
func (a *App) handleAnnouncement(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	message, updatedAt := a.cache.getSiteAnnouncement(ctx)
	b, _ := json.Marshal(map[string]any{"message": message, "updated_at": updatedAt})
	w.Write(b)
}

// handleCustomSection is polled by every visitor's page to fetch the raw
// admin-authored HTML/CSS/JS block (set via /code on Telegram) that renders
// at the bottom of the site — global, not tied to any device.
func (a *App) handleCustomSection(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	html, updatedAt := a.cache.getCustomSection(ctx)
	b, _ := json.Marshal(map[string]any{"html": html, "updated_at": updatedAt})
	w.Write(b)
}

func (a *App) handleContact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	body.Message = strings.TrimSpace(body.Message)
	if body.Message == "" {
		http.Error(w, `{"error":"message is required"}`, http.StatusBadRequest)
		return
	}
	if len(body.Name) > 100 {
		body.Name = body.Name[:100]
	}
	if len(body.Message) > 1000 {
		body.Message = body.Message[:1000]
	}
	if body.Name == "" {
		body.Name = "Anonymous visitor"
	}

	deviceID := getOrSetDeviceID(w, r)
	if !a.cache.contactMessageCooldown(ctx, deviceID) {
		http.Error(w, `{"error":"Thoda ruk ke dobara try karo (1 message/minute limit)."}`, http.StatusTooManyRequests)
		return
	}

	// Get-or-create a short numeric Access ID for this device (same system
	// used for password-protected file requests) so the admin can actually
	// /block or /unblock whoever sent this message — a raw device_id (UUID)
	// doesn't work with those commands, only the 5-digit Access ID does.
	approval, _, aErr := a.db.getOrCreateApproval(ctx, deviceID, "")
	accessIDLine := ""
	var kb tgbotapi.InlineKeyboardMarkup
	if aErr == nil && approval != nil {
		accessIDLine = fmt.Sprintf("\n🔑 Access ID: `%05d`", approval.AccessID)
		kb = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚫 Block this visitor", fmt.Sprintf("va:blk:%d", approval.AccessID)),
		))
	} else if aErr != nil {
		a.logger.Warn("handleContact: getOrCreateApproval failed", zap.Error(aErr))
	}

	text := fmt.Sprintf(
		"📩 *New website message*\n👤 %s\n🆔 Device: %s%s\n\n%s",
		mdEscape(body.Name), mdEscape(deviceID), accessIDLine, mdEscape(body.Message),
	)
	if approval != nil {
		a.pool.sendKB(a.cfg.AdminID, text, kb)
	} else {
		a.pool.sendMD(a.cfg.AdminID, text)
	}

	w.Write([]byte(`{"ok":true}`))
}

// handleMovieRequest lets a visitor request a movie/title to be added —
// just a name and a release date, nothing else. Sent straight to the
// admin's Telegram so they can go find and upload it.
func (a *App) handleMovieRequest(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Name        string `json:"name"`
		ReleaseDate string `json:"release_date"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 2048)).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	body.ReleaseDate = strings.TrimSpace(body.ReleaseDate)
	if body.Name == "" {
		http.Error(w, `{"error":"Movie ka naam daalo."}`, http.StatusBadRequest)
		return
	}
	if len(body.Name) > 150 {
		body.Name = body.Name[:150]
	}
	if len(body.ReleaseDate) > 50 {
		body.ReleaseDate = body.ReleaseDate[:50]
	}
	if body.ReleaseDate == "" {
		body.ReleaseDate = "Not specified"
	}

	deviceID := getOrSetDeviceID(w, r)
	if !a.cache.contactMessageCooldown(ctx, deviceID) {
		http.Error(w, `{"error":"Thoda ruk ke dobara try karo (1 request/minute limit)."}`, http.StatusTooManyRequests)
		return
	}

	accessIDLine := ""
	if approval, _, aErr := a.db.getOrCreateApproval(ctx, deviceID, ""); aErr == nil && approval != nil {
		accessIDLine = fmt.Sprintf("\n🔑 Access ID: %05d", approval.AccessID)
	}

	reqID := strings.ToLower(uuid.New().String()[:8])
	_ = a.db.createContentRequest(ctx, &ContentRequest{
		ID: reqID, Name: body.Name, ReleaseDate: body.ReleaseDate, DeviceID: deviceID,
	})

	text := fmt.Sprintf(
		"🎬 *Movie/Book Request*\n📽️ %s\n📅 Release: %s\n🆔 Device: %s%s",
		mdEscape(body.Name), mdEscape(body.ReleaseDate), mdEscape(deviceID), accessIDLine,
	)
	if _, ok := a.cache.getImportSource(ctx); ok {
		kb := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🔎 Find & Upload", "findreq:"+reqID),
		))
		msgCfg := tgbotapi.NewMessage(a.cfg.AdminID, text)
		msgCfg.ParseMode = "MarkdownV2"
		msgCfg.ReplyMarkup = kb
		a.pool.primary().Send(msgCfg)
	} else {
		a.pool.sendMD(a.cfg.AdminID, text)
	}

	w.Write([]byte(`{"ok":true}`))
}

// handleRedeem lets a visitor type in a premium code (given to them by the
// admin after paying, via /gencode) and unlocks premium — bypassing
// password-protection and the manual-approval gate — for their current
// browser (identified by the same device_id cookie used everywhere else).
// buildPremiumPlanCardsHTML renders the 4 pricing tiers as HTML cards for
// the Buy Premium page — each card has a data-plan attribute the page's JS
// uses to kick off a purchase request for that plan.
func buildPremiumPlanCardsHTML() string {
	var sb strings.Builder
	for _, p := range premiumPlans {
		priceHTML := fmt.Sprintf(`₹%d`, p.Price)
		if p.OrigDue > 0 {
			priceHTML = fmt.Sprintf(`<span class="strike">₹%d</span> ₹%d`, p.OrigDue, p.Price)
		}
		var benefitsHTML strings.Builder
		for _, b := range p.Benefits {
			benefitsHTML.WriteString(fmt.Sprintf(`<li>%s</li>`, html.EscapeString(b)))
		}
		highestBadge := ""
		if p.ID == "3m" {
			highestBadge = `<div class="plan-badge">BEST VALUE</div>`
		}
		sb.WriteString(fmt.Sprintf(`
    <div class="plan-card" data-plan="%s">
      %s
      <div class="plan-label">%s</div>
      <div class="plan-price">%s</div>
      <ul class="plan-benefits">%s</ul>
      <button type="button" class="plan-buy-btn" data-plan="%s">Buy Now</button>
    </div>`, p.ID, highestBadge, html.EscapeString(p.Label), priceHTML, benefitsHTML.String(), p.ID))
	}
	return sb.String()
}

// safeNextPath only allows same-site absolute paths ("/watch/abc"), so a
// ?next= value can never turn /redeem into an open redirect.
func safeNextPath(raw string) string {
	if raw == "" || raw[0] != '/' || strings.HasPrefix(raw, "//") ||
		strings.ContainsAny(raw, "\\\r\n") {
		return "/"
	}
	return raw
}

// findApprovalByDevice is a read-only lookup (unlike getOrCreateApproval it
// never inserts a record).
func (db *DB) findApprovalByDevice(ctx context.Context, deviceID string) (*ApprovalRecord, error) {
	var rec ApprovalRecord
	if err := db.approvals.FindOne(ctx, bson.M{"device_id": deviceID}).Decode(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (a *App) handleRedeem(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	_, hadDeviceCookie := r.Cookie(deviceCookieName)
	deviceID := getOrSetDeviceID(w, r)

	// Referral tracking lives here (not on a specific movie's watch page)
	// because /redeem is a permanent route — it can never 404 due to a
	// movie being deleted later, unlike a link built from a specific
	// file's watch URL.
	if hadDeviceCookie != nil {
		if refCode := strings.TrimSpace(r.URL.Query().Get("ref")); refCode != "" {
			if refAccessID, convErr := strconv.Atoi(refCode); convErr == nil {
				if refApproval, rErr := a.db.getApprovalByAccessID(ctx, refAccessID); rErr == nil && refApproval != nil {
					if _, justEarned, recErr := a.db.recordReferral(ctx, refApproval.DeviceID, deviceID); recErr == nil && justEarned {
						if _, gErr := a.db.grantPremiumDirect(ctx, refApproval.DeviceID, 1); gErr != nil {
							a.logger.Warn("referral reward grant failed", zap.Error(gErr))
						}
					}
				}
			}
		}
	}

	// Came here via a referral link / ?next= and is already premium or
	// approved by the admin? Skip the purchase page and go straight to the
	// videos.
	if r.Method == http.MethodGet && (r.URL.Query().Get("ref") != "" || r.URL.Query().Get("next") != "") {
		allowed := a.db.isPremium(ctx, deviceID)
		if !allowed {
			if rec, aErr := a.db.findApprovalByDevice(ctx, deviceID); aErr == nil && rec.Approved && !rec.Blocked {
				allowed = true
			}
		}
		if allowed {
			http.Redirect(w, r, safeNextPath(r.URL.Query().Get("next")), http.StatusSeeOther)
			return
		}
	}

	renderPage := func(message string) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		fmt.Fprintf(w, `<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ASTRATOONIX Premium</title>
<style>
  * { box-sizing: border-box; }
  body{background:#0b0d12;color:#e5e7eb;font-family:system-ui,-apple-system,sans-serif;margin:0;padding:20px;min-height:100vh;}
  .wrap{max-width:480px;margin:0 auto;}
  .back-btn{position:fixed;top:14px;left:14px;z-index:50;width:42px;height:42px;padding:0;border-radius:50%%;display:flex;align-items:center;justify-content:center;background:#161a22;border:1px solid #2a2f3a;color:#e5e7eb;font-size:20px;line-height:1;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.4);}
  .back-btn:active{transform:scale(.92);}
  .brand{text-align:center;font-weight:900;font-size:26px;letter-spacing:2px;margin-bottom:2px;
    background:linear-gradient(90deg,#ff2d75,#ff8a00,#ffe600,#3dff6e,#00e5ff,#7b2fff,#ff2d75);
    background-size:300% 100%; -webkit-background-clip:text; background-clip:text; color:transparent;
    animation:brandFlow 4s linear infinite;}
  @keyframes brandFlow{0%%{background-position:0%% 0;}100%%{background-position:300%% 0;}}
  .tagline{text-align:center;color:#9ca3af;font-size:13px;margin-bottom:22px;}
  .card{background:#161a22;border:1px solid #2a2f3a;border-radius:16px;padding:22px;margin-bottom:16px;}
  h1{font-size:18px;margin:0 0 6px;}
  p{color:#9ca3af;font-size:14px;margin:0 0 16px;}
  .msg{padding:10px 12px;border-radius:8px;margin-bottom:14px;font-size:14px;}
  .msg.ok{background:#0f2e1a;color:#4ade80;border:1px solid #16532c;}
  .msg.err{background:#2e1010;color:#f87171;border:1px solid #532020;}
  .plans{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px;}
  .plan-card{position:relative;background:#161a22;border:1px solid #2a2f3a;border-radius:14px;padding:16px 12px;text-align:center;}
  .plan-badge{position:absolute;top:-9px;left:50%%;transform:translateX(-50%%);background:linear-gradient(90deg,#ff8a00,#ffe600);color:#111;font-size:10px;font-weight:800;padding:2px 8px;border-radius:20px;letter-spacing:.03em;}
  .plan-label{font-weight:700;font-size:14px;margin-bottom:6px;}
  .plan-price{font-size:20px;font-weight:800;color:#fff;margin-bottom:10px;}
  .plan-price .strike{font-size:13px;color:#6b7280;text-decoration:line-through;font-weight:500;margin-right:4px;}
  .plan-benefits{list-style:none;padding:0;margin:0 0 14px;text-align:left;font-size:11.5px;color:#9ca3af;}
  .plan-benefits li{margin-bottom:5px;padding-left:14px;position:relative;}
  .plan-benefits li::before{content:"✓";position:absolute;left:0;color:#4ade80;font-weight:700;}
  .plan-buy-btn{width:100%%;padding:9px;border-radius:9px;border:none;background:#6366f1;color:#fff;font-weight:700;font-size:13px;cursor:pointer;}
  input{width:100%%;box-sizing:border-box;padding:12px 14px;border-radius:10px;border:1px solid #2a2f3a;background:#0b0d12;color:#e5e7eb;font-size:16px;letter-spacing:2px;text-transform:uppercase;margin-bottom:14px;}
  button{width:100%%;padding:12px;border-radius:10px;border:none;background:#6366f1;color:#fff;font-weight:700;font-size:15px;cursor:pointer;}

  #qrSection{display:none;text-align:center;}
  #qrImg{width:220px;max-width:70%%;border-radius:12px;margin:10px auto;display:block;
    opacity:0; transform:rotate(720deg) scale(0);}
  #qrImg.enter{animation:qrEntrance 1.1s cubic-bezier(.2,.8,.2,1) forwards;}
  @keyframes qrEntrance{
    0%%{opacity:0;transform:rotate(720deg) scale(0);}
    70%%{opacity:1;transform:rotate(-8deg) scale(1.06);}
    100%%{opacity:1;transform:rotate(0deg) scale(1);}
  }
  .scan-label{font-weight:800;letter-spacing:.08em;font-size:13px;color:#4ade80;margin-bottom:2px;text-transform:uppercase;}
  #qrAmount{font-size:22px;font-weight:800;margin-bottom:10px;}
  #qrStatus{font-size:13px;color:#9ca3af;margin-top:10px;}
  .spinner-dot{display:inline-block;width:8px;height:8px;border-radius:50%%;background:#6366f1;margin:0 2px;animation:dotPulse 1.2s infinite ease-in-out;}
  .spinner-dot:nth-child(2){animation-delay:.2s;}
  .spinner-dot:nth-child(3){animation-delay:.4s;}
  @keyframes dotPulse{0%%,80%%,100%%{opacity:.3;transform:scale(.8);}40%%{opacity:1;transform:scale(1.2);}}
</style></head><body>
<button type="button" class="back-btn" id="backBtn" aria-label="Back" title="Back">&#8592;</button>
<div class="wrap">
  <div class="brand">ASTRATOONIX</div>
  <div class="tagline">Premium Access</div>

  <div class="card">
    <h1>🎟️ Buy Premium</h1>
    <p>Kisi bhi plan pe tap karo, QR scan karke pay karo — admin verify karte hi is device pe automatically premium activate ho jaayega.</p>
    <div class="plans">%s</div>

    <div id="qrSection">
      <div class="scan-label">Scan to Pay</div>
      <div id="qrAmount"></div>
      <img id="qrImg" src="%s" alt="Payment QR">
      <div id="qrStatus">Payment karne ke baad admin verify karega — page khuli rakho, <span class="spinner-dot"></span><span class="spinner-dot"></span><span class="spinner-dot"></span></div>
    </div>
  </div>

  <div class="card">
    <h1>🔑 Have a Code?</h1>
    <p>Agar admin ne pehle se ek redeem code diya hai, yahan daalo.</p>
    %s
    <form method="POST">
      <input type="text" name="code" placeholder="CODE" maxlength="16" required>
      <button type="submit">Unlock Premium</button>
    </form>
  </div>
</div>
<script>
  document.getElementById('backBtn').addEventListener('click', function() {
    if (window.history.length > 1) { window.history.back(); } else { window.location.href = '/'; }
  });
  document.querySelectorAll('.plan-buy-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var planId = btn.getAttribute('data-plan');
      var amountText = btn.closest('.plan-card').querySelector('.plan-price').innerText.trim().split(' ').pop();
      btn.disabled = true;
      btn.textContent = 'Please wait...';
      fetch('/premium/request', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan_id: planId }),
      })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          if (!data.request_id) throw new Error('failed');
          document.getElementById('qrSection').style.display = 'block';
          document.getElementById('qrAmount').textContent = 'Pay ' + amountText;
          var qrImg = document.getElementById('qrImg');
          qrImg.classList.add('enter');
          document.getElementById('qrSection').scrollIntoView({ behavior: 'smooth', block: 'center' });
          pollStatus(data.request_id);
        })
        .catch(function() {
          btn.disabled = false;
          btn.textContent = 'Buy Now';
          alert('Kuch gadbad ho gayi, dobara try karo.');
        });
    });
  });

  function pollStatus(reqId) {
    var statusEl = document.getElementById('qrStatus');
    var tries = 0;
    var interval = setInterval(function() {
      tries++;
      fetch('/premium/status?req=' + reqId, { cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          if (data.status === 'approved') {
            clearInterval(interval);
            statusEl.innerHTML = '✅ Premium activated! Video page khul raha hai...';
            statusEl.style.color = '#4ade80';
            setTimeout(goNext, 1200);
          }
        })
        .catch(function() {});
      if (tries > 600) clearInterval(interval); // stop after ~30 min of polling
    }, 3000);
  }

  // Where to land once premium/approval comes through: ?next=/path if it is
  // a same-site path, otherwise the main video page.
  function goNext() {
    var n = new URLSearchParams(window.location.search).get('next') || '/';
    if (n.charAt(0) !== '/' || n.indexOf('//') === 0 || n.indexOf('\\') !== -1) n = '/';
    window.location.href = n;
  }
  // Admin pressed /approve (or premium was granted) while this page is open:
  // jump to the videos automatically — no reload needed.
  // Only for people sent here from a referral link / ?next= — someone who
  // opened /redeem on purpose (e.g. an existing premium user) is left alone.
  (function() {
    var qs = new URLSearchParams(window.location.search);
    if (!qs.get('ref') && !qs.get('next')) return;
    setInterval(function() {
      fetch('/approval-status/redeem?peek=1', { cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) { if (d.approved && !d.blocked) goNext(); })
        .catch(function() {});
    }, 4000);
  })();
</script>
</body></html>`, buildPremiumPlanCardsHTML(), a.cfg.PremiumQRURL, message)
	}

	if r.Method != http.MethodPost {
		renderPage("")
		return
	}

	code := strings.ToUpper(strings.TrimSpace(r.FormValue("code")))
	if code == "" {
		renderPage(`<div class="msg err">Code daalo.</div>`)
		return
	}

	expiresAt, err := a.db.redeemPremiumCode(ctx, code, deviceID)
	if err != nil {
		renderPage(`<div class="msg err">❌ Yeh code invalid hai ya already use ho chuka hai.</div>`)
		return
	}
	_ = expiresAt
	// Code accepted: land directly on the video page instead of leaving the
	// person on the purchase screen.
	http.Redirect(w, r, safeNextPath(r.URL.Query().Get("next")), http.StatusSeeOther)
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
		Poster    string  `json:"poster,omitempty"`
		Rating    float64 `json:"rating,omitempty"`
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
				Poster:    tmdbPoster(f),
				Rating:    tmdbRating(f),
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
// preferredQualityFile picks which quality variant of a multi-quality group
// should be treated as the "main"/default one — for the primary link a
// visitor first opens, and as the representative card shown in listings.
// Preference: 360p if available, else 480p, else whichever available
// quality is smallest (least MB) — deliberately favoring a small, quick-to-
// buffer file over the biggest one, so viewers on a weak connection get a
// playable default instead of always defaulting to the heaviest 1080p/4K
// variant.
func preferredQualityFile(files []*FileRecord) *FileRecord {
	if len(files) == 0 {
		return nil
	}
	var has360, has480 *FileRecord
	smallest := files[0]
	for _, f := range files {
		if f.QualityRank == 360 && has360 == nil {
			has360 = f
		}
		if f.QualityRank == 480 && has480 == nil {
			has480 = f
		}
		if f.QualityRank > 0 && (smallest.QualityRank == 0 || f.QualityRank < smallest.QualityRank) {
			smallest = f
		}
	}
	if has360 != nil {
		return has360
	}
	if has480 != nil {
		return has480
	}
	return smallest
}

func dedupeByGroup(files []*FileRecord) ([]*FileRecord, map[string]int) {
	groups := make(map[string][]*FileRecord)
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
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], f)
	}

	out := make([]*FileRecord, 0, len(order))
	counts := make(map[string]int, len(order))
	for _, key := range order {
		rep := preferredQualityFile(groups[key])
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
	// Fetch a larger raw batch than we need per page, since multi-quality
	// groups collapse down to one card each after dedupeByGroup — without
	// this cushion a page could come back looking short even though more
	// distinct movies exist further down.
	const rawFetchLimit = 60

	type rowItem struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Size      string `json:"size"`
		Locked    bool   `json:"locked"`
		Qualities int    `json:"qualities"`
		Poster    string  `json:"poster,omitempty"`
		Rating    float64 `json:"rating,omitempty"`
	}
	toRows := func(files []*FileRecord, limit int) []rowItem {
		deduped, counts := dedupeByGroup(files)
		if len(deduped) > limit {
			deduped = deduped[:limit]
		}
		out := make([]rowItem, 0, len(deduped))
		for _, f := range deduped {
			out = append(out, rowItem{
				ID:        f.ID,
				Name:      f.FileName,
				Size:      formatSize(f.FileSize),
				Locked:    f.PasswordHash != nil && *f.PasswordHash != "",
				Qualities: counts[f.ID],
				Poster:    tmdbPoster(f),
				Rating:    tmdbRating(f),
			})
		}
		return out
	}

	// Infinite-scroll pagination: /rows?section=trending&page=1 (or
	// section=new). page is 0-indexed and defaults to 0. When section is
	// absent, keep the original combined response (both rows, page 0) so
	// the very first page load is unchanged.
	section := r.URL.Query().Get("section")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 0 {
		page = 0
	}
	skip := page * rawFetchLimit

	if section == "trending" || section == "new" {
		var raw []*FileRecord
		var err error
		if section == "trending" {
			raw, err = a.db.topFilesByViews(ctx, skip, rawFetchLimit)
		} else {
			raw, err = a.db.newestFiles(ctx, skip, rawFetchLimit)
		}
		if err != nil {
			a.logger.Error("handleRows: paged fetch failed", zap.Error(err), zap.String("section", section))
			http.Error(w, `{"error":"fetch failed"}`, http.StatusInternalServerError)
			return
		}
		items := toRows(raw, rowLimit)
		hasMore := len(raw) >= rawFetchLimit
		b, _ := json.Marshal(map[string]any{"items": items, "has_more": hasMore})
		w.Write(b)
		return
	}

	trending, err := a.db.topFilesByViews(ctx, 0, rawFetchLimit)
	if err != nil {
		a.logger.Error("topFilesByViews failed", zap.Error(err))
	}
	newest, err := a.db.newestFiles(ctx, 0, rawFetchLimit)
	if err != nil {
		a.logger.Error("newestFiles failed", zap.Error(err))
	}

	b, _ := json.Marshal(map[string]any{
		"trending":         toRows(trending, rowLimit),
		"new_releases":     toRows(newest, rowLimit),
		"trending_more":    len(trending) >= rawFetchLimit,
		"new_releases_more": len(newest) >= rawFetchLimit,
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
		Poster    string  `json:"poster,omitempty"`
		Rating    float64 `json:"rating,omitempty"`
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
				Poster:    tmdbPoster(f),
				Rating:    tmdbRating(f),
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
		Poster    string  `json:"poster,omitempty"`
		Rating    float64 `json:"rating,omitempty"`
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
				Poster:    tmdbPoster(f),
				Rating:    tmdbRating(f),
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
		if rec, rErr := a.db.getFileByID(ctx, fid); rErr == nil && rec.GroupID != nil && *rec.GroupID != "" {
			if members, mErr := a.db.listGroupFiles(ctx, *rec.GroupID); mErr == nil {
				for _, m := range members {
					if found, err := a.db.deleteFileByID(ctx, m.ID); err == nil && found {
						a.cache.delFile(ctx, m.ID)
					}
				}
			}
		} else if found, err := a.db.deleteFileByID(ctx, fid); err == nil && found {
			a.cache.delFile(ctx, fid)
		}
		redirectBack()
		return
	}

	files, _ := a.db.countFiles(ctx)
	users, _ := a.db.countUsers(ctx)
	totalViews, _ := a.db.sumViews(ctx)
	liveNow := a.cache.liveCountAll(ctx)
	top, _ := a.db.topFilesByViews(ctx, 0, 25)
	visitors, _ := a.db.listApprovals(ctx, 30)
	tok := url.QueryEscape(token)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
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

// --- Premium plans ---
type premiumPlan struct {
	ID       string
	Label    string
	Days     int
	Price    int
	OrigDue  int // undiscounted "sticker" price for the strikethrough, 0 if no discount
	Benefits []string
}

var premiumPlans = []premiumPlan{
	{
		ID: "15d", Label: "15 Days", Days: 15, Price: 59,
		Benefits: []string{
			"All password-protected content unlocked",
			"Works instantly on this device",
		},
	},
	{
		ID: "1m", Label: "1 Month", Days: 30, Price: 99,
		Benefits: []string{
			"All password-protected content unlocked",
			"Priority reply on support messages",
		},
	},
	{
		ID: "2m", Label: "2 Months", Days: 60, Price: 179, OrigDue: 198,
		Benefits: []string{
			"All password-protected content unlocked",
			"Priority reply on support messages",
			"Early access to new uploads",
		},
	},
	{
		ID: "3m", Label: "3 Months", Days: 90, Price: 249, OrigDue: 297,
		Benefits: []string{
			"All password-protected content unlocked",
			"Priority reply on support messages",
			"Early access to new uploads",
			"Highest-tier: first access to new content + dedicated priority support",
		},
	},
}

func findPremiumPlan(id string) (premiumPlan, bool) {
	for _, p := range premiumPlans {
		if p.ID == id {
			return p, true
		}
	}
	return premiumPlan{}, false
}

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
		tgbotapi.BotCommand{Command: "tmdb", Description: "Link a file to TMDB (admin)"},
		tgbotapi.BotCommand{Command: "adlink", Description: "Set sponsor/ad link (admin)"},
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
	mux.HandleFunc("/redeem", app.handleRedeem)
	mux.HandleFunc("/contact", app.handleContact)
	mux.HandleFunc("/request-movie", app.handleMovieRequest)
	mux.HandleFunc("/announcement", app.handleAnnouncement)
	mux.HandleFunc("/custom-section", app.handleCustomSection)
	mux.HandleFunc("/qualities/", app.handleFileQualities)
	mux.HandleFunc("/meta/", app.handleMeta)
	mux.HandleFunc("/api/titles", app.handleAPITitles)
	mux.HandleFunc("/manifest.webmanifest", app.handleManifest)
	mux.HandleFunc("/icon.svg", app.handleIcon)
	mux.HandleFunc("/sw.js", app.handleServiceWorker)
	mux.HandleFunc("/api/title/", app.handleAPITitle)
	mux.HandleFunc("/team", app.handleTeam)
	mux.HandleFunc("/premium/request", app.handlePremiumRequest)
	mux.HandleFunc("/premium/status", app.handlePremiumStatus)
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
			// DropPendingUpdates is intentionally false: Koyeb's free tier
			// scale-to-zero can restart this instance mid-session, and any
			// command sent during that brief window would otherwise be
			// silently discarded as a "stale pending update" — instead we
			// let Telegram redeliver it once we're back online, so a
			// message sent during a restart still gets a reply a few
			// seconds later instead of vanishing with zero response.
			pool.primary().Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: false})
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
