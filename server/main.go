package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	pb "go-grpc-crud-api-bun/proto"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"google.golang.org/grpc"
)

func init() {
	DatabaseConnection()
}

var DB *bun.DB
var err error

type Movie struct {
	ID        string    `bun:"type:uuid,default:gen_random_uuid(),pk"`
	Title     string    `bun:",type:varchar(100)"`
	Genre     string    `bun:",type:varchar(100)"`
	CreatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

func DatabaseConnection() (*bun.DB, error) {
	host := "localhost"
	port := "54321"
	dbName := "test_db"
	dbUser := "root"
	password := "123456"

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser,
		password,
		host,
		port,
		dbName,
	)

	// Open a connection to the database
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	DB = bun.NewDB(sqldb, pgdialect.New())

	if err != nil {
		return nil, fmt.Errorf("error connecting to the database: %v", err)
	}
	fmt.Println("Database connection successful...")

	// Automatically create table if not exists
	_, err = DB.NewCreateTable().Model(&Movie{}).Exec(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return DB, nil
}

var (
	port = flag.Int("port", 50051, "gRPC server port")
)

type server struct {
	pb.UnimplementedMovieServiceServer
}

func (*server) CreateMovie(ctx context.Context, req *pb.CreateMovieRequest) (*pb.CreateMovieResponse, error) {
	fmt.Println("Create Movie")
	movie := req.GetMovie()
	movie.Id = uuid.New().String()

	data := Movie{
		ID:    movie.GetId(),
		Title: movie.GetTitle(),
		Genre: movie.GetGenre(),
	}
	fmt.Println(data)
	if DB == nil {
		fmt.Println("db is nil")
	}

	_, err := DB.NewInsert().Model(&data).Exec(context.Background())
	if err != nil {
		log.Printf("failed to create movie: %v", err)
		return nil, err
	}

	// res := DB.Create(&data)
	// if res.RowsAffected == 0 {
	// 	return nil, errors.New("movie creation unsuccessful")
	// }

	return &pb.CreateMovieResponse{
		Movie: &pb.Movie{
			Id:    movie.GetId(),
			Title: movie.GetTitle(),
			Genre: movie.GetGenre(),
		},
	}, nil
}

func (*server) GetMovie(ctx context.Context, req *pb.ReadMovieRequest) (*pb.ReadMovieResponse, error) {
	fmt.Println("Read Movie", req.GetId())

	var movie Movie
	if err := DB.NewSelect().Model(&movie).Where("id = ?", req.GetId()).Scan(ctx); err != nil {
		return nil, fmt.Errorf("failed to find movie: %v", err)
	}
	// res := DB.Find(&movie, "id = ?", req.GetId())
	// if res.RowsAffected == 0 {
	// 	return nil, errors.New("movie not found")
	// }

	return &pb.ReadMovieResponse{
		Movie: &pb.Movie{
			Id:    movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},
	}, nil
}

func (*server) GetMovies(ctx context.Context, req *pb.ReadMoviesRequest) (*pb.ReadMoviesResponse, error) {
	println("Read Movies")
	movies := []*pb.Movie{}
	res := DB.NewInsert().Model(&movies)
	if res != nil {
		return nil, errors.New("movie not found")
	}

	return &pb.ReadMoviesResponse{
		Movies: movies,
	}, nil
}

func (*server) UpdateMovie(ctx context.Context, req *pb.UpdateMovieRequest) (*pb.UpdateMovieResponse, error) {
	println("Update Movie")
	reqMovie := req.GetMovie()

	// var movie Movie
	// res := DB.Model(&movie).Where("id=?", reqMovie.Id).Updates(Movie{Title: reqMovie.Title, Genre: reqMovie.Genre})
	var movie Movie
	if err := DB.NewSelect().Model(&movie).Where("id = ?", reqMovie.Id).Scan(ctx); err != nil {
		return nil, fmt.Errorf("failed to find movie: %v", err)
	}

	if err != nil {
		return nil, errors.New("movies not found")
	}

	return &pb.UpdateMovieResponse{
		Movie: &pb.Movie{
			Id:    movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},
	}, nil
}

func (*server) DeleteMovie(ctx context.Context, req *pb.DeleteMovieRequest) (*pb.DeleteMovieResponse, error) {
	fmt.Println("Delete Movie")
	var movie Movie
	// res := DB.Where("id = ?", req.GetId()).Delete(&movie)
	res := DB.NewSelect().Model(&movie).Where("id = ?", req.GetId()).Scan(ctx)
	if res != nil {
		return nil, errors.New("movie not found")
	}

	return &pb.DeleteMovieResponse{
		Success: true,
	}, nil
}

func main() {
	fmt.Println("gRPC server running...")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()

	pb.RegisterMovieServiceServer(s, &server{})
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve : %v", err)
	}
}
